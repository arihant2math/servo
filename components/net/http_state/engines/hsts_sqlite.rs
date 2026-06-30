/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::hsts::{HstsEntry, HstsList};
use crate::http_state::engines::shared::init_connection;
use crate::http_state::hsts::HstsListEngine;

pub const DB_FILENAME: &str = "hsts_list.sqlite";

pub struct SqliteHstsListEngine {
    connection: Connection,
}

impl SqliteHstsListEngine {
    pub fn new(db_dir: Option<&Path>) -> rusqlite::Result<Self> {
        let path = db_dir.map(|dir| dir.join(DB_FILENAME));
        let connection = init_connection(path.as_deref())?;
        connection.execute(
            "CREATE TABLE IF NOT EXISTS hsts_entries (
                host TEXT PRIMARY KEY,
                base_domain TEXT NOT NULL,
                include_subdomains INTEGER NOT NULL,
                expires_at INTEGER NULL
            );",
            [],
        )?;
        connection.execute(
            "CREATE INDEX IF NOT EXISTS hsts_entries_base_domain_idx ON hsts_entries(base_domain);",
            [],
        )?;
        Ok(Self { connection })
    }

    pub fn db_path(db_dir: &Path) -> PathBuf {
        db_dir.join(DB_FILENAME)
    }
}

impl HstsListEngine for SqliteHstsListEngine {
    type Error = rusqlite::Error;

    fn load(&mut self) -> Result<HstsList, Self::Error> {
        self.connection.execute(
            "DELETE FROM hsts_entries WHERE expires_at IS NOT NULL AND expires_at <= unixepoch()",
            [],
        )?;

        let mut list = HstsList::default();
        let mut stmt = self.connection.prepare(
            "SELECT host, include_subdomains, expires_at FROM hsts_entries ORDER BY host",
        )?;
        let rows = stmt.query_map([], |row| {
            let expires_at = row.get::<_, Option<u64>>(2)?.and_then(NonZeroU64::new);
            Ok(HstsEntry {
                host: row.get(0)?,
                include_subdomains: row.get::<_, i64>(1)? != 0,
                expires_at,
            })
        })?;
        for row in rows {
            list.push(row?);
        }
        Ok(list)
    }

    fn upsert(&mut self, entry: &HstsEntry, base_domain: &str) -> Result<(), Self::Error> {
        self.connection.execute(
            "INSERT INTO hsts_entries (host, base_domain, include_subdomains, expires_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(host) DO UPDATE SET
                 base_domain = excluded.base_domain,
                 include_subdomains = excluded.include_subdomains,
                 expires_at = excluded.expires_at",
            rusqlite::params![
                &entry.host,
                base_domain,
                i64::from(entry.include_subdomains),
                entry.expires_at.map(NonZeroU64::get),
            ],
        )?;
        Ok(())
    }

    fn delete(&mut self, host: &str) -> Result<(), Self::Error> {
        self.connection
            .execute("DELETE FROM hsts_entries WHERE host = ?", [host])?;
        Ok(())
    }

    fn clear(&mut self) -> Result<(), Self::Error> {
        self.connection.execute("DELETE FROM hsts_entries", [])?;
        Ok(())
    }

    fn save(&mut self, data: &HstsList) -> Result<(), Self::Error> {
        let tx = self.connection.transaction()?;
        tx.execute("DELETE FROM hsts_entries", [])?;
        let mut stmt = tx.prepare(
            "INSERT INTO hsts_entries (host, base_domain, include_subdomains, expires_at)
             VALUES (?, ?, ?, ?)",
        )?;
        for (base_domain, entries) in &data.entries_map {
            for entry in entries {
                stmt.execute(rusqlite::params![
                    &entry.host,
                    base_domain,
                    i64::from(entry.include_subdomains),
                    entry.expires_at.map(NonZeroU64::get),
                ])?;
            }
        }
        drop(stmt);
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use tempfile::tempdir;

    use super::SqliteHstsListEngine;
    use crate::http_state::hsts::HstsListEngine;

    #[test]
    fn round_trip_and_cleanup() {
        let dir = tempdir().unwrap();
        let mut engine = SqliteHstsListEngine::new(Some(dir.path())).unwrap();
        engine
            .upsert(
                &crate::hsts::HstsEntry {
                    host: "servo.org".to_owned(),
                    include_subdomains: true,
                    expires_at: Some(NonZeroU64::new(4_102_444_800).unwrap()),
                },
                "servo.org",
            )
            .unwrap();
        engine
            .upsert(
                &crate::hsts::HstsEntry {
                    host: "expired-http-state.test".to_owned(),
                    include_subdomains: false,
                    expires_at: Some(NonZeroU64::new(1).unwrap()),
                },
                "expired-http-state.test",
            )
            .unwrap();

        let list = engine.load().unwrap();
        assert!(list.is_host_secure("servo.org"));
        assert!(!list.is_host_secure("expired-http-state.test"));
    }
}
