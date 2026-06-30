/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::http_state::auth_cache::{AuthCache, AuthCacheEngine, AuthCacheEntry};
use crate::http_state::engines::shared::init_connection;

pub const DB_FILENAME: &str = "auth_cache.sqlite";

pub struct SqliteAuthCacheEngine {
    connection: Connection,
}

impl SqliteAuthCacheEngine {
    pub fn new(db_dir: Option<&Path>) -> rusqlite::Result<Self> {
        let path = db_dir.map(|dir| dir.join(DB_FILENAME));
        let connection = init_connection(path.as_deref())?;
        connection.execute(
            "CREATE TABLE IF NOT EXISTS auth_entries (
                origin TEXT PRIMARY KEY,
                user_name TEXT NOT NULL,
                password TEXT NOT NULL
            );",
            [],
        )?;
        Ok(Self { connection })
    }

    pub fn db_path(db_dir: &Path) -> PathBuf {
        db_dir.join(DB_FILENAME)
    }
}

impl AuthCacheEngine for SqliteAuthCacheEngine {
    type Error = rusqlite::Error;

    fn load(&mut self) -> Result<AuthCache, Self::Error> {
        let mut data = AuthCache::default();
        let mut stmt = self
            .connection
            .prepare("SELECT origin, user_name, password FROM auth_entries")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                AuthCacheEntry {
                    user_name: row.get(1)?,
                    password: row.get(2)?,
                },
            ))
        })?;
        for row in rows {
            let (origin, entry) = row?;
            data.entries.insert(origin, entry);
        }
        Ok(data)
    }

    fn set(&mut self, origin: &str, entry: &AuthCacheEntry) -> Result<(), Self::Error> {
        self.connection.execute(
            "INSERT INTO auth_entries (origin, user_name, password)
             VALUES (?, ?, ?)
             ON CONFLICT(origin) DO UPDATE SET
                 user_name = excluded.user_name,
                 password = excluded.password",
            rusqlite::params![origin, &entry.user_name, &entry.password],
        )?;
        Ok(())
    }

    fn delete(&mut self, origin: &str) -> Result<(), Self::Error> {
        self.connection
            .execute("DELETE FROM auth_entries WHERE origin = ?", [origin])?;
        Ok(())
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.connection.execute("DELETE FROM auth_entries", [])?;
        Ok(())
    }

    fn save(&mut self, data: &AuthCache) -> Result<(), Self::Error> {
        let tx = self.connection.transaction()?;
        tx.execute("DELETE FROM auth_entries", [])?;
        let mut stmt =
            tx.prepare("INSERT INTO auth_entries (origin, user_name, password) VALUES (?, ?, ?)")?;
        for (origin, entry) in &data.entries {
            stmt.execute(rusqlite::params![origin, &entry.user_name, &entry.password])?;
        }
        drop(stmt);
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::SqliteAuthCacheEngine;
    use crate::http_state::auth_cache::{AuthCacheEngine, AuthCacheEntry};

    #[test]
    fn round_trip() {
        let dir = tempdir().unwrap();
        let mut engine = SqliteAuthCacheEngine::new(Some(dir.path())).unwrap();
        engine
            .set(
                "https://servo.org",
                &AuthCacheEntry {
                    user_name: "alice".to_owned(),
                    password: "secret".to_owned(),
                },
            )
            .unwrap();

        let data = engine.load().unwrap();
        let entry = data.entries.get("https://servo.org").unwrap();
        assert_eq!(entry.user_name, "alice");
        assert_eq!(entry.password, "secret");
    }
}
