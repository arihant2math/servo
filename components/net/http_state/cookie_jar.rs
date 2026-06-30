/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use cookie::Cookie;
use log::warn;
use net_traits::{CookieSource, SiteDescriptor};
use parking_lot::{Mutex, RwLock};
use servo_url::ServoUrl;

use crate::cookie::ServoCookie;
use crate::cookie_storage::CookieStorage;

pub const DEFAULT_MAX_COOKIES_PER_HOST: usize = 150;

pub trait CookieJarEngine {
    type Error: std::error::Error;

    fn load(&mut self, max_per_host: usize) -> Result<CookieStorage, Self::Error>;
    fn upsert(&mut self, bucket_domain: &str, cookie: &ServoCookie) -> Result<(), Self::Error>;
    fn delete(
        &mut self,
        bucket_domain: &str,
        name: &str,
        cookie_domain: &str,
        path: &str,
    ) -> Result<(), Self::Error>;
    fn delete_for_reg_host(&mut self, reg_host: &str) -> Result<(), Self::Error>;
    fn delete_session_cookies(&mut self) -> Result<(), Self::Error>;
    fn clear(&mut self) -> Result<(), Self::Error>;
    fn save(&mut self, data: &CookieStorage) -> Result<(), Self::Error>;
}

pub struct CookieJarStore<E> {
    data: RwLock<CookieStorage>,
    engine: Mutex<E>,
}

impl<E: CookieJarEngine> CookieJarStore<E> {
    pub fn load(mut engine: E, max_per_host: usize) -> Self {
        let data = engine.load(max_per_host).unwrap_or_else(|error| {
            warn!("Failed to load cookie jar from SQLite: {error}");
            CookieStorage::new(max_per_host)
        });
        Self::from_parts(data, engine)
    }

    pub fn from_parts(data: CookieStorage, engine: E) -> Self {
        Self {
            data: RwLock::new(data),
            engine: Mutex::new(engine),
        }
    }

    pub fn push(&self, cookie: ServoCookie, url: &ServoUrl, source: CookieSource) {
        let bucket_domain =
            crate::cookie_storage::reg_host(cookie.cookie.domain().unwrap_or_default());
        let mut jar = self.data.write();
        if !jar.push(cookie.clone(), url, source) {
            return;
        }
        if let Err(error) = self.engine.lock().upsert(&bucket_domain, &cookie) {
            warn!("Failed to persist cookie: {error}; rewriting cookie jar");
            self.save_locked(&jar);
            return;
        }
        self.save_locked(&jar);
    }

    pub fn set_cookie_for_url(&self, request: &ServoUrl, cookie_val: &str, source: CookieSource) {
        if let Some(cookie) = ServoCookie::from_cookie_string(cookie_val, request, source) {
            self.push(cookie, request, source);
        }
    }

    pub fn cookies_for_url(&self, url: &ServoUrl, source: CookieSource) -> Option<String> {
        let mut jar = self.data.write();
        jar.remove_expired_cookies_for_url(url);
        jar.cookies_for_url(url, source)
    }

    pub fn cookies_data_for_url(
        &self,
        url: &ServoUrl,
        source: CookieSource,
    ) -> Vec<Cookie<'static>> {
        let mut jar = self.data.write();
        jar.remove_expired_cookies_for_url(url);
        jar.cookies_data_for_url(url, source).collect()
    }

    pub fn query_cookies(&self, url: &ServoUrl, name: Option<String>) -> Vec<Cookie<'static>> {
        let mut jar = self.data.write();
        jar.remove_expired_cookies_for_url(url);
        jar.query_cookies(url, name)
    }

    pub fn delete_cookies_for_sites(&self, sites: &Vec<String>) {
        let mut jar = self.data.write();
        jar.delete_cookies_for_sites(sites);
        for site in sites {
            if let Err(error) = self.engine.lock().delete_for_reg_host(site) {
                warn!("Failed to delete cookies for site from SQLite: {error}");
                self.save_locked(&jar);
                return;
            }
        }
    }

    pub fn clear_session_cookies(&self) {
        self.data.write().clear_session_cookies();
        if let Err(error) = self.engine.lock().delete_session_cookies() {
            warn!("Failed to delete session cookies from SQLite: {error}");
            self.save_snapshot();
        }
    }

    pub fn clear_storage(&self, url: Option<&ServoUrl>) {
        match url {
            Some(url) => {
                let reg_host = crate::cookie_storage::reg_host(url.host_str().unwrap_or_default());
                let mut jar = self.data.write();
                jar.clear_storage(Some(url));
                if let Err(error) = self.engine.lock().delete_for_reg_host(&reg_host) {
                    warn!("Failed to delete cookies for site from SQLite: {error}");
                    self.save_locked(&jar);
                }
            },
            None => {
                self.data.write().clear_storage(None);
                if let Err(error) = self.engine.lock().clear() {
                    warn!("Failed to clear cookie jar from SQLite: {error}");
                    self.save_snapshot();
                }
            },
        }
    }

    pub fn delete_cookie_with_name(&self, url: &ServoUrl, name: String) {
        let mut jar = self.data.write();
        jar.delete_cookie_with_name(url, name);
        self.save_locked(&jar);
    }

    pub fn remove_all_expired_cookies(&self) {
        let mut jar = self.data.write();
        jar.remove_all_expired_cookies();
        self.save_locked(&jar);
    }

    pub fn cookie_site_descriptors(&self) -> Vec<SiteDescriptor> {
        self.data.read().cookie_site_descriptors()
    }

    pub fn replace_all(&self, data: CookieStorage) {
        *self.data.write() = data.clone();
        if let Err(error) = self.engine.lock().save(&data) {
            warn!("Failed to persist cookie jar snapshot: {error}");
        }
    }

    pub fn save_snapshot(&self) {
        let jar = self.data.read();
        self.save_locked(&jar);
    }

    fn save_locked(&self, jar: &CookieStorage) {
        if let Err(error) = self.engine.lock().save(jar) {
            warn!("Failed to persist cookie jar snapshot: {error}");
        }
    }
}
