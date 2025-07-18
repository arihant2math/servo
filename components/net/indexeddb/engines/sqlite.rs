/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use itertools::Itertools;
use log::error;
use net_traits::indexeddb_thread::{
    AsyncOperation, AsyncReadOnlyOperation, AsyncReadWriteOperation, CreateObjectStoreResult,
    IndexedDBKeyType, PutItemResult,
};
use sea_orm::prelude::*;
use sea_orm::{Database, NotSet, Set};
use tokio::sync::{RwLock, oneshot};

use crate::async_runtime::HANDLE;
use crate::indexeddb::engines::{KvsEngine, KvsTransaction, SanitizedName};

mod index_model;
mod metadata_model;
mod store_model;

macro_rules! err {
    ($e:expr) => {
        Err(format!("{:?}", $e))
    };
}

pub struct SqliteEngine {
    db_dir: PathBuf,
    connections: Arc<RwLock<HashMap<SanitizedName, DatabaseConnection>>>,
}

impl SqliteEngine {
    pub fn new(base_dir: &Path, db_dir_name: &Path) -> Self {
        let mut db_dir = PathBuf::new();
        db_dir.push(base_dir);
        db_dir.push(db_dir_name);
        std::fs::create_dir_all(&db_dir).expect("Could not create OS directory for idb");

        Self {
            db_dir,
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl KvsEngine for SqliteEngine {
    type Error = DbErr;

    fn create_store(
        &self,
        store_name: SanitizedName,
        auto_increment: bool,
    ) -> Result<CreateObjectStoreResult, Self::Error> {
        HANDLE.block_on(async {
            let path = PathBuf::from(&self.db_dir).join(format!("{}.db", store_name.name));
            if path.exists() {
                return Ok(CreateObjectStoreResult::AlreadyExists);
            }
            let conn = Database::connect(&format!("sqlite://{}", path.display(),)).await?;
            let builder = conn.get_database_backend();
            let schema = sea_orm::Schema::new(builder);
            let create_table_stmt =
                builder.build(&schema.create_table_from_entity(store_model::Entity));
            conn.execute(create_table_stmt).await?;
            let create_table_stmt =
                builder.build(&schema.create_table_from_entity(metadata_model::Entity));
            conn.execute(create_table_stmt).await?;
            let create_table_stmt =
                builder.build(&schema.create_table_from_entity(index_model::Entity));
            conn.execute(create_table_stmt).await?;
            if auto_increment {
                let metadata = metadata_model::ActiveModel {
                    key: Set(store_name.name.clone()),
                    value: Set(1),
                };
                metadata.insert(&conn).await?;
            } else {
                let metadata = metadata_model::ActiveModel {
                    key: Set(store_name.name.clone()),
                    value: Set(0),
                };
                metadata.insert(&conn).await?;
            }

            let mut connections = self.connections.write().await;
            connections.insert(store_name.clone(), conn);
            Ok(CreateObjectStoreResult::Created)
        })
    }

    fn delete_store(&self, store_name: SanitizedName) -> Result<(), Self::Error> {
        HANDLE.block_on(async {
            let mut connections = self.connections.write().await;
            if let Some(conn) = connections.remove(&store_name) {
                store_model::Entity::delete_many().exec(&conn).await?;
                metadata_model::Entity::delete_many().exec(&conn).await?;
                conn.close().await?;
                let db_path = self.db_dir.join(format!("{}.db", store_name.name));
                if db_path.exists() {
                    if let Err(e) = std::fs::remove_file(db_path) {
                        error!("Could not remove existing indexeddb store: {:?}", e);
                    }
                }
            }
            Ok(())
        })
    }

    fn close_store(&self, store_name: SanitizedName) -> Result<(), Self::Error> {
        HANDLE.block_on(async {
            let mut connections = self.connections.write().await;
            if let Some(conn) = connections.remove(&store_name) {
                conn.close().await?;
            }
            Ok(())
        })
    }

    fn delete_database(&self) -> Result<(), Self::Error> {
        HANDLE.block_on(async {
            let mut connections = self.connections.write().await;
            for (store_name, conn) in connections.drain() {
                store_model::Entity::delete_many().exec(&conn).await?;
                metadata_model::Entity::delete_many().exec(&conn).await?;
                conn.close().await?;
                let db_path = self.db_dir.join(format!("{}.db", store_name.name));
                if db_path.exists() {
                    std::fs::remove_file(db_path).expect("Failed to delete database file");
                }
            }
            Ok(())
        })
    }

    fn process_transaction(
        &self,
        transaction: KvsTransaction,
    ) -> oneshot::Receiver<Option<Vec<u8>>> {
        let (tx, rx) = oneshot::channel();
        let connections = self.connections.clone();

        // TODO: maybe use different pools for different transactions?
        HANDLE.spawn(async move {
            for request in transaction.requests {
                let connections_reader = connections.read().await;
                let conn = match connections_reader.get(&request.store_name) {
                    Some(conn) => conn,
                    None => {
                        // TODO: This is also kinda wrong, but atleast we don't panic.
                        tx.send(None).unwrap_or(());
                        return;
                    },
                };

                match request.operation {
                    AsyncOperation::ReadWrite(AsyncReadWriteOperation::PutItem {
                        sender,
                        key,
                        value,
                        should_overwrite,
                    }) => {
                        let serialized_key: Vec<u8> = match bincode::serialize(&key) {
                            Ok(key) => key,
                            Err(e) => {
                                let _ = sender.send(err!(e));
                                break;
                            },
                        };
                        let store = store_model::ActiveModel {
                            id: NotSet,
                            key: Set(serialized_key.clone()),
                            value: Set(value),
                        };
                        if should_overwrite ||
                            store_model::Entity::find()
                                .filter(store_model::Column::Key.eq(serialized_key.clone()))
                                .one(conn)
                                .await
                                .unwrap() // TODO: handle
                                .is_none()
                        {
                            match store.insert(conn).await {
                                Ok(_) => {
                                    let _ = sender.send(Ok(PutItemResult::Success));
                                },
                                Err(e) => {
                                    let _ = sender.send(err!(e));
                                },
                            }
                        } else {
                            let _ = sender.send(Ok(PutItemResult::CannotOverwrite));
                        }
                    },
                    AsyncOperation::ReadOnly(AsyncReadOnlyOperation::GetItem { sender, key }) => {
                        let serialized_key: Vec<u8> = bincode::serialize(&key).unwrap();
                        let result = store_model::Entity::find()
                            .filter(store_model::Column::Key.eq(serialized_key))
                            .one(conn)
                            .await;

                        match result {
                            Ok(result) => {
                                let _ = sender.send(Ok(result.map(|blob| blob.value.to_vec())));
                            },
                            Err(e) => {
                                let _ = sender.send(err!(e));
                            },
                        }
                    },
                    AsyncOperation::ReadWrite(AsyncReadWriteOperation::RemoveItem {
                        sender,
                        key,
                    }) => {
                        let serialized_key: Vec<u8> = bincode::serialize(&key).unwrap();
                        // More ergonomic way to delete an item than querying first.
                        let result = store_model::Entity::delete_many()
                            .filter(store_model::Column::Key.eq(serialized_key))
                            .exec(conn)
                            .await;
                        if let Err(err) = result {
                            let _ = sender.send(err!(err));
                        } else {
                            let _ = sender.send(Ok(()));
                        }
                    },
                    AsyncOperation::ReadOnly(AsyncReadOnlyOperation::Count {
                        sender,
                        key_range,
                    }) => {
                        let res = store_model::Entity::find().all(conn).await;
                        match res {
                            Ok(list) => {
                                let count = list
                                    .iter()
                                    .filter(|s| {
                                        let key: IndexedDBKeyType =
                                            bincode::deserialize(&s.key).unwrap();
                                        key_range.contains(&key)
                                    })
                                    .try_len()
                                    .unwrap_or(0);
                                // TODO: make that return usize instead of u64
                                let _ = sender.send(Ok(count as u64));
                            },
                            Err(e) => {
                                let _ = sender.send(err!(e));
                            },
                        }
                    },
                    AsyncOperation::ReadWrite(AsyncReadWriteOperation::Clear(sender)) => {
                        let result = store_model::Entity::delete_many().exec(conn).await;
                        let _ = match result {
                            Ok(_) => sender.send(Ok(())),
                            Err(e) => sender.send(err!(e)),
                        };
                    },
                }
            }
        });
        rx
    }

    // TODO: we should be able to error out here, maybe change the trait definition?
    fn has_key_generator(&self, store_name: SanitizedName) -> bool {
        HANDLE.block_on(async {
            let connections = self.connections.clone();
            let connections = connections.read().await;
            if let Some(conn) = connections.get(&store_name) {
                let metadata = metadata_model::Entity::find()
                    .filter(metadata_model::Column::Key.eq(store_name.name.clone()))
                    .one(conn)
                    .await
                    .unwrap();
                if let Some(metadata) = metadata {
                    return metadata.value > 0;
                }
            }
            false
        })
    }
}
