/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */
use std::collections::HashMap;
use std::fmt::Debug;

use ipc_channel::ipc::{IpcReceiver, IpcSender};
use net_traits::indexeddb_thread::{
    CreateObjectResult, IndexedDBTransactionState, IndexedDBTxnMode, KeyPath, KvsOperation,
};

pub use self::sqlite::SqliteEngine;

mod sqlite;

pub struct KvsTransaction {
    // Mode could be used by a more optimal implementation of transactions
    // that has different allocated threadpools for reading and writing
    #[allow(unused)]
    pub mode: IndexedDBTxnMode,
    pub stores: Vec<String>,
    pub start: chrono::DateTime<chrono::Utc>,
    pub state_change_sender: IpcSender<IndexedDBTransactionState>,
    pub operations: IpcReceiver<KvsOperation>,
}

impl Debug for KvsTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvsTransaction")
            .field("mode", &self.mode)
            .field("stores", &self.stores)
            .field("start", &self.start)
            .finish()
    }
}

pub trait KvsEngine {
    type Error: std::error::Error;

    fn create_store(
        &self,
        store_name: &str,
        key_path: Option<KeyPath>,
        auto_increment: bool,
    ) -> Result<CreateObjectResult, Self::Error>;

    fn delete_store(&self, store_name: &str) -> Result<(), Self::Error>;

    fn close_store(&self, store_name: &str) -> Result<(), Self::Error>;

    fn locked_store_names(&self) -> HashMap<String, IndexedDBTxnMode>;

    fn delete_database(self) -> Result<(), Self::Error>;

    fn process_transaction(&self, transaction: KvsTransaction);

    fn has_key_generator(&self, store_name: &str) -> bool;
    fn key_path(&self, store_name: &str) -> Option<KeyPath>;

    fn create_index(
        &self,
        store_name: &str,
        index_name: String,
        key_path: KeyPath,
        unique: bool,
        multi_entry: bool,
    ) -> Result<CreateObjectResult, Self::Error>;
    fn delete_index(&self, store_name: &str, index_name: String) -> Result<(), Self::Error>;

    fn version(&self) -> Result<u64, Self::Error>;
    fn set_version(&self, version: u64) -> Result<(), Self::Error>;
}
