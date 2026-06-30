/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::path::Path;

use rusqlite::Connection;

const DB_INIT_PRAGMAS: [&str; 2] = ["PRAGMA journal_mode = WAL;", "PRAGMA encoding = 'UTF-16';"];
const DB_IN_MEMORY_INIT_PRAGMAS: [&str; 1] = ["PRAGMA encoding = 'UTF-16';"];
const DB_PRAGMAS: [&str; 4] = [
    "PRAGMA synchronous = NORMAL;",
    "PRAGMA journal_size_limit = 67108864 -- 64 megabytes;",
    "PRAGMA mmap_size = 67108864 -- 64 megabytes;",
    "PRAGMA cache_size = 2000;",
];
const DB_IN_MEMORY_PRAGMAS: [&str; 1] = ["PRAGMA cache_size = 2000;"];
const SCHEMA_VERSION: i64 = 1;

pub fn init_connection(db_path: Option<&Path>) -> rusqlite::Result<Connection> {
    let connection = if let Some(path) = db_path {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let connection = Connection::open(path)?;
        for pragma in DB_INIT_PRAGMAS {
            let _ = connection.execute(pragma, []);
        }
        for pragma in DB_PRAGMAS {
            let _ = connection.execute(pragma, []);
        }
        connection
    } else {
        let connection = Connection::open_in_memory()?;
        for pragma in DB_IN_MEMORY_INIT_PRAGMAS {
            let _ = connection.execute(pragma, []);
        }
        for pragma in DB_IN_MEMORY_PRAGMAS {
            let _ = connection.execute(pragma, []);
        }
        connection
    };

    connection.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    Ok(connection)
}
