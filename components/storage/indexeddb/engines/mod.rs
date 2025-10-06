/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use storage_traits::indexeddb_thread::{CreateObjectResult, IndexedDBTxnMode, KeyPath, TransactionReceiver};
use tokio::sync::oneshot;

pub use self::sqlite::SqliteEngine;

mod sqlite;

pub struct KvsTransaction {
    // Mode could be used by a more optimal implementation of transactions
    // that has different allocated threadpools for reading and writing
    #[allow(unused)]
    pub mode: IndexedDBTxnMode,
    pub stores: Vec<String>,
    pub receiver: TransactionReceiver,
}

// TODO: not actually safe
unsafe impl Send for KvsTransaction {}
unsafe impl Sync for KvsTransaction {}

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

    fn delete_database(self) -> Result<(), Self::Error>;

    fn process_transaction(
        &self,
        transaction: KvsTransaction,
    ) -> oneshot::Receiver<Option<Vec<u8>>>;

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
