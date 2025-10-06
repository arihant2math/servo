/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use std::borrow::ToOwned;
use std::collections::hash_map::Entry;
use std::collections::{HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::thread::JoinHandle;
use dashmap::DashMap;
use base::threadpool::ThreadPool;
use ipc_channel::ipc::{self, IpcError, IpcReceiver, IpcSender};
use log::{debug, warn};
use servo_config::pref;
use servo_url::origin::ImmutableOrigin;
use storage_traits::indexeddb_thread::{create_transaction_channel, BackendError, BackendResult, CreateObjectResult, DbResult, IndexedDBThreadMsg, IndexedDBTxnMode, KeyPath, SyncOperation, TransactionSender};
use uuid::Uuid;

use crate::indexeddb::engines::{KvsEngine, KvsTransaction, SqliteEngine};

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
    const NAMESPACE_SERVO_IDB: &uuid::Uuid = &Uuid::from_bytes([
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

type Notifier = Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>;

struct IndexedDBScheduler<E: KvsEngine> {
    engine: Arc<E>,
    // Maps transaction id to the transaction mode
    pending_transactions: Arc<DashMap<u64, KvsTransaction>>,
    shutdown: Arc<AtomicBool>,
    new_transaction_notifier: Notifier,
}

impl<E: KvsEngine> IndexedDBScheduler<E> {
    fn new(
        engine: Arc<E>,
        pending_transactions: Arc<DashMap<u64, KvsTransaction>>,
        shutdown: Arc<AtomicBool>,
        new_transaction_notifier: Notifier,
    ) -> IndexedDBScheduler<E> {
        IndexedDBScheduler {
            engine,
            pending_transactions,
            shutdown,
            new_transaction_notifier,
        }
    }

    fn start(self) {
        // This scheduler takes the pending transactions and processes them one by one
        // in the order of their transaction id (which is monotonically increasing)
        // it waits for them to finish before processing the next one
        while !self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            if let Some(txn) = self
                .pending_transactions
                .iter()
                .min_by_key(|t| *t.key()) {
                // Remove the transaction from the pending list
                let txn = self.pending_transactions.remove(&txn.key()).unwrap().1;
                let _ = self.engine.process_transaction(txn).blocking_recv();
            } else {
                // wait for new transaction
                let (lock, cvar) = &*self.new_transaction_notifier;
                let mut started = lock.lock().unwrap();
                while !*started {
                    started = cvar.wait(started).unwrap();
                }
                *started = false;
                continue;
            }
        }
    }
}

struct IndexedDBEnvironment<E: KvsEngine> {
    engine: Arc<E>,
    transactions: Arc<DashMap<u64, KvsTransaction>>,
    serial_number_counter: u64,
    scheduling_thread: JoinHandle<()>,
    shutdown: Arc<AtomicBool>,
    new_transaction_notifier: Notifier,
}

impl<E: KvsEngine + Send + Sync + 'static> IndexedDBEnvironment<E> {
    fn new(engine: E) -> IndexedDBEnvironment<E> {
        let engine = Arc::new(engine);
        let transactions = Arc::new(DashMap::default());
        let shutdown = Arc::new(AtomicBool::new(false));
        let new_transaction_notifier = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let scheduling_thread = thread::spawn({
            let engine = engine.clone();
            let transactions = transactions.clone();
            let shutdown = shutdown.clone();
            let new_transaction_notifier = new_transaction_notifier.clone();
            || {
                IndexedDBScheduler::new(
                    engine,
                    transactions,
                    shutdown,
                    new_transaction_notifier,
                )
                .start();
            }
        });
        IndexedDBEnvironment {
            engine,
            transactions,
            serial_number_counter: 0,
            scheduling_thread,
            shutdown,
            new_transaction_notifier,
        }
    }

    fn create_transaction(&mut self, mode: IndexedDBTxnMode, stores: Vec<String>) -> TransactionSender {
        self.serial_number_counter += 1;
        let (sender, receiver) = create_transaction_channel();
        let txn = KvsTransaction { mode, stores, receiver };
        self.transactions.insert(self.serial_number_counter, txn);
        // Notify the scheduling thread that a new transaction is available
        let (lock, cvar) = &*self.new_transaction_notifier;
        let mut started = lock.lock().unwrap();
        *started = true;
        cvar.notify_one();
        sender
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
        // Signal the scheduling thread to shutdown
        self.shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
        // Wait for the scheduling thread to finish
        let _ = self.scheduling_thread.join();
        // Now it is safe to delete the database
        let engine = Arc::into_inner(self.engine).unwrap();
        let result = engine.delete_database();
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
    thread_pool: Arc<ThreadPool>,
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
            thread_pool: Arc::new(ThreadPool::new(thread_count, "IndexedDB".to_string())),
        }
    }
}

impl IndexedDBManager {
    fn start(&mut self) {
        loop {
            // FIXME:(arihant2math) No message *most likely* means that
            // the ipc sender has been dropped, so we break the look
            let message = match self.port.recv() {
                Ok(msg) => msg,
                Err(e) => match e {
                    IpcError::Disconnected => {
                        break;
                    },
                    other => {
                        warn!("Error in IndexedDB thread: {:?}", other);
                        continue;
                    },
                },
            };
            match message {
                IndexedDBThreadMsg::Sync(operation) => {
                    self.handle_sync_operation(operation);
                },
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

    fn handle_sync_operation(&mut self, operation: SyncOperation) {
        match operation {
            SyncOperation::CloseDatabase(sender, origin, db_name) => {
                let idb_description = IndexedDBDescription {
                    origin,
                    name: db_name,
                };
                if let Some(_db) = self.databases.remove(&idb_description) {
                    // TODO: maybe a close database function should be added to the trait and called here?
                }
                let _ = sender.send(Ok(()));
            },
            SyncOperation::OpenDatabase(sender, origin, db_name, version) => {
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
            SyncOperation::DeleteDatabase(sender, origin, db_name) => {
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
            SyncOperation::HasKeyGenerator(sender, origin, db_name, store_name) => {
                let result = self
                    .get_database(origin, db_name)
                    .map(|db| db.has_key_generator(&store_name));
                let _ = sender.send(result.ok_or(BackendError::DbNotFound));
            },
            SyncOperation::KeyPath(sender, origin, db_name, store_name) => {
                let result = self
                    .get_database(origin, db_name)
                    .map(|db| db.key_path(&store_name));
                let _ = sender.send(result.ok_or(BackendError::DbNotFound));
            },
            SyncOperation::CreateIndex(
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
            SyncOperation::DeleteIndex(sender, origin, db_name, store_name, index_name) => {
                if let Some(db) = self.get_database(origin, db_name) {
                    let result = db.delete_index(&store_name, index_name);
                    let _ = sender.send(result.map_err(BackendError::from));
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            SyncOperation::Commit(sender, _origin, _db_name, _txn) => {
                // FIXME:(arihant2math) This does nothing at the moment
                let _ = sender.send(Ok(()));
            },
            SyncOperation::CreateObjectStore(
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
            SyncOperation::DeleteObjectStore(sender, origin, db_name, store_name) => {
                if let Some(db) = self.get_database_mut(origin, db_name) {
                    let result = db.delete_object_store(&store_name);
                    let _ = sender.send(result.map_err(BackendError::from));
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            SyncOperation::Version(sender, origin, db_name) => {
                if let Some(db) = self.get_database(origin, db_name) {
                    let _ = sender.send(db.version().map_err(BackendError::from));
                } else {
                    let _ = sender.send(Err(BackendError::DbNotFound));
                }
            },
            SyncOperation::CreateTransaction(sender, origin, db_name, stores, mode) => {
                if let Some(db) = self.get_database_mut(origin, db_name) {
                    let txn_sender = db.create_transaction(mode, stores);
                    let _ = sender.send(txn_sender);
                }
            },
            SyncOperation::Exit(sender) => {
                // FIXME:(rasviitanen) Nothing to do?
                let _ = sender.send(());
            },
        }
    }
}
