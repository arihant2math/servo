/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use headers::{HeaderMapExt, StrictTransportSecurity};
use http::HeaderMap;
use log::{info, warn};
use malloc_size_of::{MallocSizeOf, MallocSizeOfOps};
use net_traits::IncludeSubdomains;
use net_traits::pub_domains::reg_suffix;
use parking_lot::{Mutex, RwLock};
use servo_url::ServoUrl;

use crate::hsts::{HstsEntry, HstsList};

pub trait HstsListEngine {
    type Error: std::error::Error;

    fn load(&mut self) -> Result<HstsList, Self::Error>;
    fn upsert(&mut self, entry: &HstsEntry, base_domain: &str) -> Result<(), Self::Error>;
    fn delete(&mut self, host: &str) -> Result<(), Self::Error>;
    fn clear(&mut self) -> Result<(), Self::Error>;
    fn save(&mut self, data: &HstsList) -> Result<(), Self::Error>;
}

pub struct HstsStore<E> {
    data: RwLock<HstsList>,
    engine: Mutex<E>,
}

impl<E: HstsListEngine> HstsStore<E> {
    pub fn load(mut engine: E) -> Self {
        let data = engine.load().unwrap_or_else(|error| {
            warn!("Failed to load HSTS list from SQLite: {error}");
            HstsList::default()
        });
        Self::from_parts(data, engine)
    }

    pub fn from_parts(data: HstsList, engine: E) -> Self {
        Self {
            data: RwLock::new(data),
            engine: Mutex::new(engine),
        }
    }

    pub fn is_host_secure(&self, host: &str) -> bool {
        self.data.read().is_host_secure(host)
    }

    pub fn apply_hsts_rules(&self, url: &mut ServoUrl) {
        self.data.read().apply_hsts_rules(url);
    }

    pub fn push(&self, entry: HstsEntry) {
        self.data.write().push(entry.clone());
        self.persist_entry(&entry);
    }

    pub fn update_from_response(&self, url: &ServoUrl, headers: &HeaderMap) {
        if url.scheme() != "https" && url.scheme() != "wss" {
            return;
        }

        let Some(header) = headers.typed_get::<StrictTransportSecurity>() else {
            return;
        };
        let Some(host) = url.domain() else {
            return;
        };

        let include_subdomains = if header.include_subdomains() {
            IncludeSubdomains::Included
        } else {
            IncludeSubdomains::NotIncluded
        };

        let Some(entry) =
            HstsEntry::new(host.to_owned(), include_subdomains, Some(header.max_age()))
        else {
            return;
        };

        info!("adding host {} to the strict transport security list", host);
        info!("- max-age {}", header.max_age().as_secs());
        if header.include_subdomains() {
            info!("- includeSubdomains");
        }

        self.data.write().push(entry.clone());
        self.persist_entry(&entry);
    }

    pub fn clear(&self) {
        self.data.write().entries_map.clear();
        if let Err(error) = self.engine.lock().clear() {
            warn!("Failed to clear HSTS list: {error}");
        }
    }

    pub fn replace_all(&self, data: HstsList) {
        *self.data.write() = data.clone();
        if let Err(error) = self.engine.lock().save(&data) {
            warn!("Failed to persist HSTS list snapshot: {error}");
        }
    }

    pub fn size_of(&self, ops: &mut MallocSizeOfOps) -> usize {
        self.data.read().size_of(ops)
    }

    fn persist_entry(&self, entry: &HstsEntry) {
        let mut engine = self.engine.lock();
        if entry.is_expired() {
            if let Err(error) = engine.delete(&entry.host) {
                warn!("Failed to delete expired HSTS entry: {error}");
            }
            return;
        }

        let base_domain = reg_suffix(&entry.host).to_owned();
        if let Err(error) = engine.upsert(entry, &base_domain) {
            warn!("Failed to persist HSTS entry: {error}");
        }
    }
}
