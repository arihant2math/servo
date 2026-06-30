/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

use crate::cookie::ServoCookie;
use crate::cookie_storage::CookieStorage;
use crate::http_state::cookie_jar::CookieJarEngine;
use crate::http_state::engines::shared::init_connection;

pub const DB_FILENAME: &str = "cookie_jar.sqlite";

pub struct SqliteCookieJarEngine {
    connection: Connection,
}

impl SqliteCookieJarEngine {
    pub fn new(db_dir: Option<&Path>) -> rusqlite::Result<Self> {
        let path = db_dir.map(|dir| dir.join(DB_FILENAME));
        let connection = init_connection(path.as_deref())?;
        connection.execute(
            "CREATE TABLE IF NOT EXISTS cookies (
                bucket_domain TEXT NOT NULL,
                name TEXT NOT NULL,
                cookie_domain TEXT NOT NULL,
                path TEXT NOT NULL,
                persistent INTEGER NOT NULL,
                expiry_time INTEGER NULL,
                payload TEXT NOT NULL,
                PRIMARY KEY (bucket_domain, name, cookie_domain, path)
            );",
            [],
        )?;
        connection.execute(
            "CREATE INDEX IF NOT EXISTS cookies_bucket_domain_idx ON cookies(bucket_domain);",
            [],
        )?;
        Ok(Self { connection })
    }

    pub fn db_path(db_dir: &Path) -> PathBuf {
        db_dir.join(DB_FILENAME)
    }
}

impl CookieJarEngine for SqliteCookieJarEngine {
    type Error = rusqlite::Error;

    fn load(&mut self, max_per_host: usize) -> Result<CookieStorage, Self::Error> {
        self.delete_session_cookies()?;
        self.connection.execute(
            "DELETE FROM cookies WHERE expiry_time IS NOT NULL AND expiry_time <= unixepoch()",
            [],
        )?;

        let mut storage = CookieStorage::new(max_per_host);
        let mut stmt = self
            .connection
            .prepare("SELECT payload FROM cookies ORDER BY bucket_domain, name, path")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            let cookie = serde_json::from_str::<ServoCookie>(&row?)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
            storage.push_persisted(cookie);
        }
        Ok(storage)
    }

    fn upsert(&mut self, bucket_domain: &str, cookie: &ServoCookie) -> Result<(), Self::Error> {
        let cookie_domain = cookie.cookie.domain().unwrap_or_default();
        let path = cookie.cookie.path().unwrap_or_default();
        let payload = serde_json::to_string(cookie)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        self.connection.execute(
            "INSERT INTO cookies (
                bucket_domain,
                name,
                cookie_domain,
                path,
                persistent,
                expiry_time,
                payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bucket_domain, name, cookie_domain, path) DO UPDATE SET
                persistent = excluded.persistent,
                expiry_time = excluded.expiry_time,
                payload = excluded.payload",
            rusqlite::params![
                bucket_domain,
                cookie.cookie.name(),
                cookie_domain,
                path,
                i64::from(cookie.persistent),
                system_time_to_unix(cookie.expiry_time),
                payload,
            ],
        )?;
        Ok(())
    }

    fn delete(
        &mut self,
        bucket_domain: &str,
        name: &str,
        cookie_domain: &str,
        path: &str,
    ) -> Result<(), Self::Error> {
        self.connection.execute(
            "DELETE FROM cookies
             WHERE bucket_domain = ? AND name = ? AND cookie_domain = ? AND path = ?",
            rusqlite::params![bucket_domain, name, cookie_domain, path],
        )?;
        Ok(())
    }

    fn delete_for_reg_host(&mut self, reg_host: &str) -> Result<(), Self::Error> {
        self.connection
            .execute("DELETE FROM cookies WHERE bucket_domain = ?", [reg_host])?;
        Ok(())
    }

    fn delete_session_cookies(&mut self) -> Result<(), Self::Error> {
        self.connection
            .execute("DELETE FROM cookies WHERE persistent = 0", [])?;
        Ok(())
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.connection.execute("DELETE FROM cookies", [])?;
        Ok(())
    }

    fn save(&mut self, data: &CookieStorage) -> Result<(), Self::Error> {
        let tx = self.connection.transaction()?;
        tx.execute("DELETE FROM cookies", [])?;
        let mut stmt = tx.prepare(
            "INSERT INTO cookies (
                bucket_domain,
                name,
                cookie_domain,
                path,
                persistent,
                expiry_time,
                payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )?;
        for (bucket_domain, cookies) in &data.cookies_map {
            for cookie in cookies {
                let cookie_domain = cookie.cookie.domain().unwrap_or_default();
                let path = cookie.cookie.path().unwrap_or_default();
                let payload = serde_json::to_string(cookie)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                stmt.execute(rusqlite::params![
                    bucket_domain,
                    cookie.cookie.name(),
                    cookie_domain,
                    path,
                    i64::from(cookie.persistent),
                    system_time_to_unix(cookie.expiry_time),
                    payload,
                ])?;
            }
        }
        drop(stmt);
        tx.commit()?;
        Ok(())
    }
}

fn system_time_to_unix(time: Option<SystemTime>) -> Option<i64> {
    time.map(|time| match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs().try_into().unwrap_or(i64::MAX),
        Err(error) => {
            let duration = error.duration();
            -(duration.as_secs().try_into().unwrap_or(i64::MAX))
        },
    })
}

#[cfg(test)]
mod tests {
    use cookie::Cookie;
    use net_traits::CookieSource;
    use servo_url::ServoUrl;
    use tempfile::tempdir;

    use super::SqliteCookieJarEngine;
    use crate::cookie::ServoCookie;
    use crate::http_state::cookie_jar::CookieJarEngine;

    #[test]
    fn round_trip_and_session_clear() {
        let dir = tempdir().unwrap();
        let mut engine = SqliteCookieJarEngine::new(Some(dir.path())).unwrap();
        let url = ServoUrl::parse("https://servo.org/").unwrap();

        let persistent = ServoCookie::from_cookie_string(
            "persistent=yes; Max-Age=3600; Path=/",
            &url,
            CookieSource::HTTP,
        )
        .unwrap();
        let session = ServoCookie::new_wrapped(
            Cookie::new("session".to_owned(), "yes".to_owned()),
            &url,
            CookieSource::HTTP,
        )
        .unwrap();

        engine.upsert("servo.org", &persistent).unwrap();
        engine.upsert("servo.org", &session).unwrap();
        engine.delete_session_cookies().unwrap();

        let mut storage = engine.load(150).unwrap();
        let cookie_string = storage.cookies_for_url(&url, CookieSource::HTTP).unwrap();
        assert_eq!(cookie_string, "persistent=yes");
    }
}
