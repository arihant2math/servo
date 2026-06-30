/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::collections::HashMap;

use log::warn;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuthCacheEntry {
    pub user_name: String,
    pub password: String,
}

impl Default for AuthCache {
    fn default() -> Self {
        Self {
            version: 1,
            entries: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AuthCache {
    pub version: u32,
    pub entries: HashMap<String, AuthCacheEntry>,
}

pub trait AuthCacheEngine {
    type Error: std::error::Error;

    fn load(&mut self) -> Result<AuthCache, Self::Error>;
    fn set(&mut self, origin: &str, entry: &AuthCacheEntry) -> Result<(), Self::Error>;
    fn delete(&mut self, origin: &str) -> Result<(), Self::Error>;
    fn clear(&mut self) -> Result<(), Self::Error>;
    fn save(&mut self, data: &AuthCache) -> Result<(), Self::Error>;
}

pub struct AuthCacheStore<E> {
    data: RwLock<AuthCache>,
    engine: Mutex<E>,
}

impl<E: AuthCacheEngine> AuthCacheStore<E> {
    pub fn load(mut engine: E) -> Self {
        let data = engine.load().unwrap_or_else(|error| {
            warn!("Failed to load auth cache from SQLite: {error}");
            AuthCache::default()
        });
        Self::from_parts(data, engine)
    }

    pub fn from_parts(data: AuthCache, engine: E) -> Self {
        Self {
            data: RwLock::new(data),
            engine: Mutex::new(engine),
        }
    }

    pub fn get(&self, origin: &str) -> Option<AuthCacheEntry> {
        self.data.read().entries.get(origin).cloned()
    }

    pub fn set(&self, origin: String, entry: AuthCacheEntry) {
        self.data
            .write()
            .entries
            .insert(origin.clone(), entry.clone());
        if let Err(error) = self.engine.lock().set(&origin, &entry) {
            warn!("Failed to persist auth cache entry: {error}");
        }
    }

    pub fn delete(&self, origin: &str) {
        self.data.write().entries.remove(origin);
        if let Err(error) = self.engine.lock().delete(origin) {
            warn!("Failed to delete auth cache entry: {error}");
        }
    }

    pub fn clear(&self) {
        self.data.write().entries.clear();
        if let Err(error) = self.engine.lock().clear() {
            warn!("Failed to clear auth cache: {error}");
        }
    }

    pub fn replace_all(&self, data: AuthCache) {
        *self.data.write() = data.clone();
        if let Err(error) = self.engine.lock().save(&data) {
            warn!("Failed to persist auth cache snapshot: {error}");
        }
    }
}
