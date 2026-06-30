/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::path::Path;

use log::warn;

use crate::cookie_storage::CookieStorage;
use crate::hsts::HstsList;

pub mod auth_cache;
pub mod cookie_jar;
pub mod engines;
pub mod hsts;

use auth_cache::AuthCacheStore;
pub use auth_cache::{AuthCache, AuthCacheEntry};
use cookie_jar::{CookieJarStore, DEFAULT_MAX_COOKIES_PER_HOST};
use engines::auth_cache_sqlite::{DB_FILENAME as AUTH_CACHE_DB_FILENAME, SqliteAuthCacheEngine};
use engines::cookie_jar_sqlite::{DB_FILENAME as COOKIE_JAR_DB_FILENAME, SqliteCookieJarEngine};
use engines::hsts_sqlite::{DB_FILENAME as HSTS_DB_FILENAME, SqliteHstsListEngine};
use hsts::HstsStore;

pub type SqliteAuthCacheStore = AuthCacheStore<SqliteAuthCacheEngine>;
pub type SqliteHstsStore = HstsStore<SqliteHstsListEngine>;
pub type SqliteCookieJarStore = CookieJarStore<SqliteCookieJarEngine>;

pub struct HttpStateStores {
    pub auth_cache: SqliteAuthCacheStore,
    pub hsts_list: SqliteHstsStore,
    pub cookie_jar: SqliteCookieJarStore,
}

pub fn create_http_state_stores(config_dir: Option<&Path>) -> HttpStateStores {
    let auth_cache = load_auth_cache_store(config_dir);
    let hsts_list = load_hsts_store(config_dir);
    let cookie_jar = load_cookie_jar_store(config_dir);

    HttpStateStores {
        auth_cache,
        hsts_list,
        cookie_jar,
    }
}

fn load_auth_cache_store(config_dir: Option<&Path>) -> SqliteAuthCacheStore {
    let had_sqlite = config_dir.is_some_and(|dir| dir.join(AUTH_CACHE_DB_FILENAME).exists());
    let engine = create_auth_cache_engine(config_dir);
    let store = SqliteAuthCacheStore::load(engine);

    if !had_sqlite
        && let Some(config_dir) = config_dir.filter(|dir| dir.join("auth_cache.json").exists())
    {
        let mut legacy = AuthCache::default();
        servo_base::read_json_from_file(&mut legacy, config_dir, "auth_cache.json");
        store.replace_all(legacy);
    }

    store
}

fn load_hsts_store(config_dir: Option<&Path>) -> SqliteHstsStore {
    let had_sqlite = config_dir.is_some_and(|dir| dir.join(HSTS_DB_FILENAME).exists());
    let engine = create_hsts_engine(config_dir);
    let store = SqliteHstsStore::load(engine);

    if !had_sqlite
        && let Some(config_dir) = config_dir.filter(|dir| dir.join("hsts_list.json").exists())
    {
        let mut legacy = HstsList::default();
        servo_base::read_json_from_file(&mut legacy, config_dir, "hsts_list.json");
        store.replace_all(legacy);
    }

    store
}

fn load_cookie_jar_store(config_dir: Option<&Path>) -> SqliteCookieJarStore {
    let had_sqlite = config_dir.is_some_and(|dir| dir.join(COOKIE_JAR_DB_FILENAME).exists());
    let engine = create_cookie_jar_engine(config_dir);
    let store = SqliteCookieJarStore::load(engine, DEFAULT_MAX_COOKIES_PER_HOST);

    if !had_sqlite
        && let Some(config_dir) = config_dir.filter(|dir| dir.join("cookie_jar.json").exists())
    {
        let mut legacy = CookieStorage::new(DEFAULT_MAX_COOKIES_PER_HOST);
        servo_base::read_json_from_file(&mut legacy, config_dir, "cookie_jar.json");
        store.replace_all(legacy);
    }

    store
}

fn create_auth_cache_engine(config_dir: Option<&Path>) -> SqliteAuthCacheEngine {
    SqliteAuthCacheEngine::new(config_dir).unwrap_or_else(|error| {
        warn!("Failed to initialize auth cache SQLite engine: {error:?}. Falling back to in-memory state.");
        SqliteAuthCacheEngine::new(None).expect("failed to initialize in-memory auth cache SQLite engine")
    })
}

fn create_hsts_engine(config_dir: Option<&Path>) -> SqliteHstsListEngine {
    SqliteHstsListEngine::new(config_dir).unwrap_or_else(|error| {
        warn!(
            "Failed to initialize HSTS SQLite engine: {error:?}. Falling back to in-memory state."
        );
        SqliteHstsListEngine::new(None).expect("failed to initialize in-memory HSTS SQLite engine")
    })
}

fn create_cookie_jar_engine(config_dir: Option<&Path>) -> SqliteCookieJarEngine {
    SqliteCookieJarEngine::new(config_dir).unwrap_or_else(|error| {
        warn!("Failed to initialize cookie jar SQLite engine: {error:?}. Falling back to in-memory state.");
        SqliteCookieJarEngine::new(None).expect("failed to initialize in-memory cookie jar SQLite engine")
    })
}

#[cfg(test)]
mod tests {
    use cookie::Cookie;
    use net_traits::{CookieSource, IncludeSubdomains};
    use tempfile::tempdir;

    use super::create_http_state_stores;
    use crate::cookie::ServoCookie;
    use crate::hsts::HstsEntry;

    #[test]
    fn imports_legacy_json_when_sqlite_is_missing() {
        let dir = tempdir().unwrap();

        let mut auth_cache = crate::http_state::AuthCache::default();
        auth_cache.entries.insert(
            "https://servo.org".to_owned(),
            crate::http_state::AuthCacheEntry {
                user_name: "alice".to_owned(),
                password: "secret".to_owned(),
            },
        );
        servo_base::write_json_to_file(&auth_cache, dir.path(), "auth_cache.json");

        let stores = create_http_state_stores(Some(dir.path()));
        let entry = stores.auth_cache.get("https://servo.org").unwrap();
        assert_eq!(entry.user_name, "alice");
        assert!(dir.path().join(super::AUTH_CACHE_DB_FILENAME).exists());
    }

    #[test]
    fn sqlite_takes_precedence_over_legacy_json() {
        let dir = tempdir().unwrap();
        let stores = create_http_state_stores(Some(dir.path()));
        stores.auth_cache.set(
            "https://servo.org".to_owned(),
            crate::http_state::AuthCacheEntry {
                user_name: "sqlite".to_owned(),
                password: "wins".to_owned(),
            },
        );

        let mut legacy = crate::http_state::AuthCache::default();
        legacy.entries.insert(
            "https://servo.org".to_owned(),
            crate::http_state::AuthCacheEntry {
                user_name: "json".to_owned(),
                password: "loses".to_owned(),
            },
        );
        servo_base::write_json_to_file(&legacy, dir.path(), "auth_cache.json");

        let stores = create_http_state_stores(Some(dir.path()));
        let entry = stores.auth_cache.get("https://servo.org").unwrap();
        assert_eq!(entry.user_name, "sqlite");
        assert_eq!(entry.password, "wins");
    }

    #[test]
    fn public_state_persists_and_private_state_does_not() {
        let dir = tempdir().unwrap();
        let stores = create_http_state_stores(Some(dir.path()));
        stores.auth_cache.set(
            "https://servo.org".to_owned(),
            crate::http_state::AuthCacheEntry {
                user_name: "alice".to_owned(),
                password: "secret".to_owned(),
            },
        );
        stores.hsts_list.push(
            HstsEntry::new("servo.org".to_owned(), IncludeSubdomains::NotIncluded, None).unwrap(),
        );
        let url = servo_url::ServoUrl::parse("https://servo.org/").unwrap();
        let cookie = ServoCookie::new_wrapped(
            Cookie::new("session".to_owned(), "cookie".to_owned()),
            &url,
            CookieSource::HTTP,
        )
        .unwrap();
        stores.cookie_jar.push(cookie, &url, CookieSource::HTTP);

        let stores = create_http_state_stores(Some(dir.path()));
        assert!(stores.auth_cache.get("https://servo.org").is_some());
        assert!(stores.hsts_list.is_host_secure("servo.org"));
        assert_eq!(
            stores.cookie_jar.cookies_for_url(&url, CookieSource::HTTP),
            None,
        );

        let private = create_http_state_stores(None);
        private.auth_cache.set(
            "https://servo.org".to_owned(),
            crate::http_state::AuthCacheEntry {
                user_name: "private".to_owned(),
                password: "secret".to_owned(),
            },
        );
        private.hsts_list.push(
            HstsEntry::new(
                "private-http-state.test".to_owned(),
                IncludeSubdomains::NotIncluded,
                None,
            )
            .unwrap(),
        );
        let private_cookie = ServoCookie::new_wrapped(
            Cookie::new("persistent".to_owned(), "cookie".to_owned()),
            &url,
            CookieSource::HTTP,
        )
        .unwrap();
        private
            .cookie_jar
            .push(private_cookie, &url, CookieSource::HTTP);

        let private = create_http_state_stores(None);
        assert!(private.auth_cache.get("https://servo.org").is_none());
        assert!(!private.hsts_list.is_host_secure("private-http-state.test"));
        assert_eq!(
            private.cookie_jar.cookies_for_url(&url, CookieSource::HTTP),
            None,
        );
    }
}
