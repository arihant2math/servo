/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use std::borrow::ToOwned;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use ipc_channel::ipc::{self, IpcError, IpcReceiver, IpcSender, TryRecvError};
use log::{debug, warn};
use net_traits::indexeddb_thread::{
    BackendError, BackendResult, CreateObjectResult, DbResult, IndexedDBThreadMsg,
    IndexedDBTxnMode, KeyPath,
};
use servo_config::pref;
use servo_url::origin::ImmutableOrigin;
use uuid::Uuid;

use crate::indexeddb::engines::{KvsEngine, KvsTransaction, SqliteEngine};
use crate::resource_thread::CoreResourceThreadPool;

pub trait IndexedDBThreadFactory {
    fn new(config_dir: Option<PathBuf>) -> Self;
}

impl IndexedDBThreadFactory for IpcSender<IndexedDBThreadMsg> {
    fn new(config_dir: Option<PathBuf>) -> IpcSender<IndexedDBThreadMsg> {
        let (chan, port) = ipc::channel().unwrap();

        let mut idb_base_dir = PathBuf::new();
        if let Some(p) = config_dir {
            idb_base_dir.push(p);
        }
        idb_base_dir.push("IndexedDB");

        thread::Builder::new()
            .name("IndexedDBManager".to_owned())
            .spawn(move || {
                IndexedDBManager::new(port, idb_base_dir).start();
            })
            .expect("Thread spawning failed");

        chan
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct IndexedDBDescription {
    pub origin: ImmutableOrigin,
    pub name: String,
}

impl IndexedDBDescription {
    // randomly generated namespace for our purposes
    const NAMESPACE_SERVO_IDB: &'static Uuid = &Uuid::from_bytes([
        0x37, 0x9e, 0x56, 0xb0, 0x1a, 0x76, 0x44, 0xc2, 0xa0, 0xdb, 0xe2, 0x18, 0xc5, 0xc8, 0xa3,
        0x5d,
    ]);
    // Converts the database description to a folder name where all
    // data for this database is stored
    pub(super) fn as_path(&self) -> PathBuf {
        let mut path = PathBuf::new();

        // uuid v5 is deterministic
        let origin_uuid = Uuid::new_v5(
            Self::NAMESPACE_SERVO_IDB,
            self.origin.ascii_serialization().as_bytes(),
        );
        let db_name_uuid = Uuid::new_v5(Self::NAMESPACE_SERVO_IDB, self.name.as_bytes());
        path.push(origin_uuid.to_string());
        path.push(db_name_uuid.to_string());

        path
    }
}

struct IndexedDBEnvironment<E: KvsEngine> {
    engine: E,
    transactions: Vec<KvsTransaction>,
}

impl<E: KvsEngine> IndexedDBEnvironment<E> {
    fn new(engine: E) -> IndexedDBEnvironment<E> {
        IndexedDBEnvironment {
            engine,
            transactions: Vec::default(),
        }
    }

    /// Queues the earliest possible non-conflicting transaction, if found for execution
    fn start_transaction(&mut self) {
        // FIXME(arihant2math): this starves write transactions if there is a constant stream of read transactions
        // Find the earliest transaction that can be started
        let locked_stores = self.engine.locked_store_names();
        let to_start_index = self.transactions.iter().position(|txn| {
            if txn.mode == IndexedDBTxnMode::Readonly {
                // Readonly transactions can run if no store they access is locked in a non-readonly mode
                !txn.stores
                    .iter()
                    .filter(|&store| locked_stores.contains_key(store))
                    .any(|store| locked_stores.get(store) != Some(&IndexedDBTxnMode::Readonly))
            } else {
                // Readwrite transactions can run if no store they access is locked
                !txn.stores
                    .iter()
                    .any(|store| locked_stores.contains_key(store))
            }
        });
        if let Some(index) = to_start_index {
            let txn = self.transactions.remove(index);
            self.engine.process_transaction(txn);
        }
    }

    fn has_key_generator(&self, store_name: &str) -> bool {
        self.engine.has_key_generator(store_name)
    }

    fn key_path(&self, store_name: &str) -> Option<KeyPath> {
        self.engine.key_path(store_name)
    }

    fn create_index(
        &self,
        store_name: &str,
        index_name: String,
        key_path: KeyPath,
        unique: bool,
        multi_entry: bool,
    ) -> DbResult<CreateObjectResult> {
        self.engine
            .create_index(store_name, index_name, key_path, unique, multi_entry)
            .map_err(|err| format!("{err:?}"))
    }

    fn delete_index(&self, store_name: &str, index_name: String) -> DbResult<()> {
        self.engine
            .delete_index(store_name, index_name)
            .map_err(|err| format!("{err:?}"))
    }

    fn create_object_store(
        &mut self,
        store_name: &str,
        key_path: Option<KeyPath>,
        auto_increment: bool,
    ) -> DbResult<CreateObjectResult> {
        self.engine
            .create_store(store_name, key_path, auto_increment)
            .map_err(|err| format!("{err:?}"))
    }

    fn delete_object_store(&mut self, store_name: &str) -> DbResult<()> {
        let result = self.engine.delete_store(store_name);
        result.map_err(|err| format!("{err:?}"))
    }

    fn delete_database(self, sender: IpcSender<BackendResult<()>>) {
        let result = self.engine.delete_database();
        let _ = sender.send(
            result
                .map_err(|err| format!("{err:?}"))
                .map_err(BackendError::from),
        );
    }

    fn version(&self) -> DbResult<u64> {
        self.engine.version().map_err(|err| format!("{err:?}"))
    }

    fn set_version(&mut self, version: u64) -> DbResult<()> {
        self.engine
            .set_version(version)
            .map_err(|err| format!("{err:?}"))
    }
}

struct IndexedDBManager {
    port: IpcReceiver<IndexedDBThreadMsg>,
    idb_base_dir: PathBuf,
    databases: HashMap<IndexedDBDescription, IndexedDBEnvironment<SqliteEngine>>,
    thread_pool: Arc<CoreResourceThreadPool>,
}

impl IndexedDBManager {
    fn new(port: IpcReceiver<IndexedDBThreadMsg>, idb_base_dir: PathBuf) -> IndexedDBManager {
        debug!("New indexedDBManager");

        // Uses an estimate of the system cpus to process IndexedDB transactions
        // See https://doc.rust-lang.org/stable/std/thread/fn.available_parallelism.html
        // If no information can be obtained about the system, uses 4 threads as a default
        let thread_count = thread::available_parallelism()
            .map(|i| i.get())
            .unwrap_or(pref!(threadpools_fallback_worker_num) as usize)
            .min(pref!(threadpools_indexeddb_workers_max).max(1) as usize);

        IndexedDBManager {
            port,
            idb_base_dir,
            databases: HashMap::new(),
            thread_pool: Arc::new(CoreResourceThreadPool::new(
                thread_count,
                "IndexedDB".to_string(),
            )),
        }
    }
}

impl IndexedDBManager {
    fn start(&mut self) {
        loop {
            let message = match self
                .port
                .try_recv_timeout(std::time::Duration::from_millis(100))
            {
                Ok(msg) => Some(msg),
                Err(TryRecvError::IpcError(IpcError::Disconnected)) => {
                    // No message *most likely* means that the ipc sender has been dropped, so we break the loop
                    break;
                },
                Err(TryRecvError::IpcError(e)) => {
                    warn!("Error in IndexedDB thread: {:?}", e);
                    None
                },
                Err(TryRecvError::Empty) => None,
            };
            if let Some(message) = message {
                self.handle_operation(message);
            } else {
                // No message, try to start queued transactions
                for (_, db) in self.databases.iter_mut() {
                    db.start_transaction();
                }
                std::hint::spin_loop()
            }
        }
    }

    fn get_database(
        &self,
        origin: ImmutableOrigin,
        db_name: String,
    ) -> Option<&IndexedDBEnvironment<SqliteEngine>> {
        let idb_description = IndexedDBDescription {
            origin,
            name: db_name,
        };

        self.databases.get(&idb_description)
    }

    fn get_database_mut(
        &mut self,
        origin: ImmutableOrigin,
        db_name: String,
    ) -> Option<&mut IndexedDBEnvironment<SqliteEngine>> {
        let idb_description = IndexedDBDescription {
            origin,
            name: db_name,
        };

        self.databases.get_mut(&idb_description)
    }

    fn handle_operation(&mut self, operation: IndexedDBThreadMsg) {
        match operation {
            IndexedDBThreadMsg::CloseDatabase(sender, origin, db_name) => {
                let idb_description = IndexedDBDescription {
                    origin,
                    name: db_name,
                };
                if let Some(_db) = self.databases.remove(&idb_description) {
                    // TODO: maybe a close database function should be added to the trait and called here?
                }
                let _ = sender.send(Ok(()));
            },
            IndexedDBThreadMsg::OpenDatabase(sender, origin, db_name, version) => {
                let idb_description = IndexedDBDescription {
                    origin,
                    name: db_name,
                };

                let idb_base_dir = self.idb_base_dir.as_path();

                let version = version.unwrap_or(0);

                match self.databases.entry(idb_description.clone()) {
                    Entry::Vacant(e) => {
                        let db = IndexedDBEnvironment::new(
                            SqliteEngine::new(
                                idb_base_dir,
                                &idb_description,
                                self.thread_pool.clone(),
                            )
                            .expect("Failed to create sqlite engine"),
                        );
                        let _ = sender.send(db.version().unwrap_or(version));
                        e.insert(db);
                    },
                    Entry::Occupied(db) => {
                        let _ = sender.send(db.get().version().unwrap_or(version));
                    },
                }
            },
            IndexedDBThreadMsg::DeleteDatabase(sender, origin, db_name) => {
                // https://w3c.github.io/IndexedDB/#delete-a-database
                // Step 4. Let db be the database named name in storageKey,
                // if one exists. Otherwise, return 0 (zero).
                let idb_description = IndexedDBDescription {
                    origin,
                    name: db_name,
                };
                if let Some(db) = self.databases.remove(&idb_description) {
                    db.delete_database(sender);
                } else {
                    let _ = sender.send(Ok(()));
                }
            },
            IndexedDBThreadMsg::HasKeyGenerator(sender, origin, db_name, store_name) => {
                let result = self
                    .get_database(origin, db_name)
                    .map(|db| db.has_key_generator(&store_name));
                let _ = sender.send(result.ok_or(BackendError::DbNotFound));
            },
            IndexedDBThreadMsg::KeyPath(sender, origin, db_name, store_name) => {
                let result = self
                    .get_database(origin, db_name)
                    .map(|db| db.key_path(&store_name));
                let _ = sender.send(result.ok_or(BackendError::DbNotFound));
            },
            IndexedDBThreadMsg::CreateIndex(
                sender,
                origin,
                db_name,
                store_name,
                index_name,
                key_path,
                unique,
                multi_entry,
            ) => {
                if let Some(db) = self.get_database(origin, db_name) {
                    let result =
                        db.create_index(&store_name, index_name, key_path, unique, multi_entry);
                    let _ = sender.send(result.map_err(BackendError::from));
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            IndexedDBThreadMsg::DeleteIndex(sender, origin, db_name, store_name, index_name) => {
                if let Some(db) = self.get_database(origin, db_name) {
                    let result = db.delete_index(&store_name, index_name);
                    let _ = sender.send(result.map_err(BackendError::from));
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            IndexedDBThreadMsg::UpgradeVersion(sender, origin, db_name, version) => {
                if let Some(db) = self.get_database_mut(origin, db_name) {
                    if version > db.version().unwrap_or(0) {
                        let _ = db.set_version(version);
                    }
                    // erroring out if the version is not upgraded can be and non-replicable
                    let _ = sender.send(db.version().map_err(BackendError::from));
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            IndexedDBThreadMsg::CreateObjectStore(
                sender,
                origin,
                db_name,
                store_name,
                key_paths,
                auto_increment,
            ) => {
                if let Some(db) = self.get_database_mut(origin, db_name) {
                    let result = db.create_object_store(&store_name, key_paths, auto_increment);
                    let _ = sender.send(result.map_err(BackendError::from));
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            IndexedDBThreadMsg::DeleteObjectStore(sender, origin, db_name, store_name) => {
                if let Some(db) = self.get_database_mut(origin, db_name) {
                    let result = db.delete_object_store(&store_name);
                    let _ = sender.send(result.map_err(BackendError::from));
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            IndexedDBThreadMsg::Version(sender, origin, db_name) => {
                if let Some(db) = self.get_database(origin, db_name) {
                    let _ = sender.send(db.version().map_err(BackendError::from));
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            IndexedDBThreadMsg::RegisterNewTxn(sender, origin, db_name, scope, txn_mode) => {
                if let Some(db) = self.get_database_mut(origin, db_name) {
                    let (state_change_sender, state_change_receiver) = ipc::channel().unwrap();
                    let (operation_sender, operation_receiver) = ipc::channel().unwrap();
                    let _ = sender.send(Ok((state_change_receiver, operation_sender)));
                    let txn = KvsTransaction {
                        mode: txn_mode,
                        stores: scope,
                        state_change_sender,
                        operations: operation_receiver,
                        start: chrono::Utc::now(),
                    };
                    db.transactions.push(txn);
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            IndexedDBThreadMsg::Exit(sender) => {
                // FIXME:(rasviitanen) Nothing to do?
                let _ = sender.send(());
            },
        }
    }
}
