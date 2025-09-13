/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::cell::Cell;
use std::collections::HashMap;

use dom_struct::dom_struct;
use ipc_channel::ipc::{IpcReceiver, IpcSender};
use net_traits::IpcSend;
use net_traits::indexeddb_thread::{
    IndexedDBThreadMsg, IndexedDBTransactionState,
    IndexedDBTxnMode, KeyPath, KvsOperation,
};
use profile_traits::ipc;
use script_bindings::codegen::GenericUnionTypes::StringOrStringSequence;
use stylo_atoms::Atom;

use crate::dom::bindings::cell::DomRefCell;
use crate::dom::bindings::codegen::Bindings::DOMStringListBinding::DOMStringListMethods;
use crate::dom::bindings::codegen::Bindings::IDBDatabaseBinding::IDBObjectStoreParameters;
use crate::dom::bindings::codegen::Bindings::IDBTransactionBinding::{
    IDBTransactionMethods, IDBTransactionMode,
};
use crate::dom::bindings::error::{Error, Fallible};
use crate::dom::bindings::inheritance::Castable;
use crate::dom::bindings::refcounted::Trusted;
use crate::dom::bindings::reflector::{DomGlobal, reflect_dom_object};
use crate::dom::bindings::root::{Dom, DomRoot, MutNullableDom};
use crate::dom::bindings::str::DOMString;
use crate::dom::domexception::DOMException;
use crate::dom::domstringlist::DOMStringList;
use crate::dom::event::{Event, EventBubbles, EventCancelable};
use crate::dom::eventtarget::EventTarget;
use crate::dom::globalscope::GlobalScope;
use crate::dom::idbdatabase::IDBDatabase;
use crate::dom::idbobjectstore::IDBObjectStore;
use crate::dom::idbrequest::IDBRequest;
use crate::script_runtime::CanGc;

#[dom_struct]
pub struct IDBTransaction {
    eventtarget: EventTarget,
    object_store_names: Dom<DOMStringList>,
    mode: IDBTransactionMode,
    db: Dom<IDBDatabase>,
    error: MutNullableDom<DOMException>,

    store_handles: DomRefCell<HashMap<String, Dom<IDBObjectStore>>>,
    // https://www.w3.org/TR/IndexedDB-2/#transaction-request-list
    requests: DomRefCell<Vec<Dom<IDBRequest>>>,
    #[no_trace]
    state: Cell<IndexedDBTransactionState>,
    force_active: Cell<bool>,
    #[ignore_malloc_size_of = "Channels are hard"]
    #[no_trace]
    operation_queue: IpcSender<KvsOperation>,
    #[ignore_malloc_size_of = "Channels are hard"]
    #[no_trace]
    state_receiver: IpcReceiver<IndexedDBTransactionState>,
}

impl IDBTransaction {
    fn new_inherited(
        connection: &IDBDatabase,
        mode: IDBTransactionMode,
        scope: &DOMStringList,
        state_receiver: IpcReceiver<IndexedDBTransactionState>,
        operation_queue: IpcSender<KvsOperation>,
    ) -> IDBTransaction {
        IDBTransaction {
            eventtarget: EventTarget::new_inherited(),
            object_store_names: Dom::from_ref(scope),
            mode,
            db: Dom::from_ref(connection),
            error: Default::default(),

            store_handles: Default::default(),
            requests: Default::default(),
            state: Cell::new(IndexedDBTransactionState::Active),
            force_active: Cell::new(true),
            operation_queue,
            state_receiver,
        }
    }

    pub fn new(
        global: &GlobalScope,
        connection: &IDBDatabase,
        mode: IDBTransactionMode,
        scope: &DOMStringList,
        can_gc: CanGc,
    ) -> DomRoot<IDBTransaction> {
        let (state_receiver, operation_queue) =
            IDBTransaction::register_new(global, connection.get_name(), scope, mode);
        reflect_dom_object(
            Box::new(IDBTransaction::new_inherited(
                connection,
                mode,
                scope,
                state_receiver,
                operation_queue,
            )),
            global,
            can_gc,
        )
    }

    // Registers a new transaction in the idb thread, and gets an unique serial number in return.
    // The serial number is used when placing requests against a transaction
    // and allows us to commit/abort transactions running in our idb thread.
    fn register_new(
        global: &GlobalScope,
        db_name: DOMString,
        scope: &DOMStringList,
        mode: IDBTransactionMode,
    ) -> (
        IpcReceiver<IndexedDBTransactionState>,
        IpcSender<KvsOperation>,
    ) {
        let (sender, receiver) = ipc::channel(global.time_profiler_chan().clone()).unwrap();

        let mode = match mode {
            IDBTransactionMode::Readonly => IndexedDBTxnMode::Readonly,
            IDBTransactionMode::Readwrite => IndexedDBTxnMode::Readwrite,
            IDBTransactionMode::Versionchange => IndexedDBTxnMode::Versionchange,
        };

        global
            .resource_threads()
            .send(IndexedDBThreadMsg::RegisterNewTxn(
                sender,
                global.origin().immutable().clone(),
                db_name.to_string(),
                scope
                    .inner()
                    .clone()
                    .iter()
                    .map(DOMString::to_string)
                    .collect(),
                mode,
            ))
            .unwrap();

        receiver.recv().unwrap().unwrap()
    }

    pub fn state(&self) -> IndexedDBTransactionState {
        while let Ok(state) = self.state_receiver.try_recv() {
            self.state.set(state);
            if matches!(state, IndexedDBTransactionState::Active) {
                panic!("Transaction went back to active state");
            }
        }
        self.state.get()
    }

    pub fn set_active_flag(&self, status: bool) {
        self.force_active.set(status)
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self.state(),
            IndexedDBTransactionState::Active | IndexedDBTransactionState::Started
        ) || self.force_active.get()
    }

    pub fn is_finished(&self) -> bool {
        matches!(
            self.state(),
            IndexedDBTransactionState::Finished | IndexedDBTransactionState::Aborted
        )
    }

    pub fn has_started(&self) -> bool {
        !matches!(self.state(), IndexedDBTransactionState::Active)
    }

    pub fn get_mode(&self) -> IDBTransactionMode {
        self.mode
    }

    pub fn add_request(&self, request: &IDBRequest) {
        self.requests.borrow_mut().push(Dom::from_ref(request));
    }

    pub fn upgrade_db_version(&self, version: u64) {
        // Runs the previous request and waits for them to finish
        self.wait();
        // Queue a request to upgrade the db version
        let (sender, receiver) = ipc::channel(self.global().time_profiler_chan().clone()).unwrap();
        let upgrade_version_operation = IndexedDBThreadMsg::UpgradeVersion(
            sender,
            self.global().origin().immutable().clone(),
            self.db.get_name().to_string(),
            version,
        );
        self.global()
            .resource_threads()
            .send(upgrade_version_operation)
            .unwrap();
        // Wait for the version to be updated
        // TODO(jdm): This returns a Result; what do we do with an error?
        let _ = receiver.recv().unwrap();
    }

    fn dispatch_complete(&self) {
        let global = self.global();
        let this = Trusted::new(self);
        global.task_manager().database_access_task_source().queue(
            task!(send_complete_notification: move || {
                let this = this.root();
                let global = this.global();
                let event = Event::new(
                    &global,
                    Atom::from("complete"),
                    EventBubbles::DoesNotBubble,
                    EventCancelable::NotCancelable,
                    CanGc::note()
                );
                event.fire(this.upcast(), CanGc::note());
            }),
        );
    }

    fn object_store_parameters(
        &self,
        object_store_name: &DOMString,
    ) -> Option<IDBObjectStoreParameters> {
        let global = self.global();
        let idb_sender = global.resource_threads().sender();
        let (sender, receiver) =
            ipc::channel(global.time_profiler_chan().clone()).expect("failed to create channel");

        let origin = global.origin().immutable().clone();
        let db_name = self.db.get_name().to_string();
        let object_store_name = object_store_name.to_string();

        let operation = IndexedDBThreadMsg::HasKeyGenerator(
            sender,
            origin.clone(),
            db_name.clone(),
            object_store_name.clone(),
        );

        let _ = idb_sender.send(operation);

        // First unwrap for ipc
        // Second unwrap will never happen unless this db gets manually deleted somehow
        let auto_increment = receiver.recv().ok()?.ok()?;

        let (sender, receiver) = ipc::channel(self.global().time_profiler_chan().clone()).ok()?;
        let operation = IndexedDBThreadMsg::KeyPath(sender, origin, db_name, object_store_name);

        let _ = idb_sender.send(operation);

        // First unwrap for ipc
        // Second unwrap will never happen unless this db gets manually deleted somehow
        let key_path = receiver.recv().unwrap().ok()?;
        let key_path = key_path.map(|key_path| match key_path {
            KeyPath::String(s) => StringOrStringSequence::String(DOMString::from_string(s)),
            KeyPath::Sequence(seq) => StringOrStringSequence::StringSequence(
                seq.into_iter().map(DOMString::from_string).collect(),
            ),
        });
        Some(IDBObjectStoreParameters {
            autoIncrement: auto_increment,
            keyPath: key_path,
        })
    }

    pub fn wait(&self) {
        let (sender, receiver) = ipc::channel(self.global().time_profiler_chan().clone()).unwrap();
        let _ = self.operation_queue.send(KvsOperation::Wait(sender));
        let _ = receiver.recv().unwrap();
    }

    pub(crate) fn queue_operation(&self, operation: KvsOperation) {
        self.operation_queue.send(operation).unwrap();
    }

    pub(crate) fn try_commit(&self) {
        if !self.requests.borrow().iter().any(|r| !r.is_finished()) {
            let _ = self.Commit();
        }
    }
}

impl IDBTransactionMethods<crate::DomTypeHolder> for IDBTransaction {
    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-db
    fn Db(&self) -> DomRoot<IDBDatabase> {
        DomRoot::from_ref(&*self.db)
    }

    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-objectstore
    fn ObjectStore(&self, name: DOMString) -> Fallible<DomRoot<IDBObjectStore>> {
        // Step 1: If transaction has finished, throw an "InvalidStateError" DOMException.
        if self.is_finished() {
            return Err(Error::InvalidState);
        }

        // Step 2: Check that the object store exists
        if !self.object_store_names.Contains(name.clone()) {
            return Err(Error::NotFound);
        }

        // Step 3: Each call to this method on the same
        // IDBTransaction instance with the same name
        // returns the same IDBObjectStore instance.
        let mut store_handles = self.store_handles.borrow_mut();
        let store = store_handles.entry(name.to_string()).or_insert_with(|| {
            let parameters = self.object_store_parameters(&name);
            let store = IDBObjectStore::new(
                &self.global(),
                self.db.get_name(),
                name,
                parameters.as_ref(),
                CanGc::note(),
                self,
            );
            Dom::from_ref(&*store)
        });

        Ok(DomRoot::from_ref(&*store))
    }

    // https://www.w3.org/TR/IndexedDB-2/#commit-transaction
    fn Commit(&self) -> Fallible<()> {
        // Step 1
        let (sender, receiver) = ipc::channel(self.global().time_profiler_chan().clone()).unwrap();

        let _ = self.operation_queue.send(KvsOperation::Commit(sender));

        let result = receiver.recv().unwrap();

        // TODO: Fix
        // Step 2
        if let Err(_result) = result {
            // FIXME:(rasviitanen) also support Unknown error
            return Err(Error::QuotaExceeded {
                quota: None,
                requested: None,
            });
        }

        // Step 3
        // FIXME:(rasviitanen) https://www.w3.org/TR/IndexedDB-2/#commit-a-transaction

        // Steps 3.1 and 3.3
        self.dispatch_complete();

        Ok(())
    }

    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-abort
    fn Abort(&self) -> Fallible<()> {
        // FIXME:(rasviitanen)
        // This only sets the flags, and does not abort the transaction
        // see https://www.w3.org/TR/IndexedDB-2/#abort-a-transaction
        if self.is_finished() {
            return Err(Error::InvalidState);
        }

        self.state.set(IndexedDBTransactionState::Finished);

        Ok(())
    }

    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-objectstorenames
    fn ObjectStoreNames(&self) -> DomRoot<DOMStringList> {
        self.object_store_names.as_rooted()
    }

    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-mode
    fn Mode(&self) -> IDBTransactionMode {
        self.mode
    }

    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-mode
    // fn Durability(&self) -> IDBTransactionDurability {
    //     // FIXME:(arihant2math) Durability is not implemented at all
    //     unimplemented!();
    // }

    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-error
    fn GetError(&self) -> Option<DomRoot<DOMException>> {
        self.error.get()
    }

    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-onabort
    event_handler!(abort, GetOnabort, SetOnabort);

    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-oncomplete
    event_handler!(complete, GetOncomplete, SetOncomplete);

    // https://www.w3.org/TR/IndexedDB-2/#dom-idbtransaction-onerror
    event_handler!(error, GetOnerror, SetOnerror);
}
