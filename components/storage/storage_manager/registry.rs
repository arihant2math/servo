use std::path::PathBuf;
use rusqlite::{Connection, OptionalExtension};


const CREATE_SHELVES_TABLE: &str = r#"CREATE TABLE shelves (
id INTEGER PRIMARY KEY,
origin TEXT NOT NULL UNIQUE
);"#;

const CREATE_BUCKETS_TABLE: &str = r#"
CREATE TABLE buckets (
id INTEGER PRIMARY KEY,
shelf_id INTEGER NOT NULL REFERENCES shelves(id),
name TEXT NOT NULL,
persisted BOOLEAN DEFAULT 0,
quota INTEGER,
expires INTEGER,
UNIQUE (shelf_id, name)
);"#;

const CREATE_BOTTLES_TABLE: &str = r#"
CREATE TABLE bottles (
id INTEGER PRIMARY KEY,
bucket_id INTEGER NOT NULL REFERENCES buckets(id),
identifier TEXT NOT NULL,  -- "idb", "ls", "opfs", "cache"
quota INTEGER,
UNIQUE (bucket_id, identifier)
);"#;

const CREATE_DATABASES_TABLE: &str = r#"
CREATE TABLE databases (
id INTEGER PRIMARY KEY,
bottle_id INTEGER NOT NULL REFERENCES bottles(id),
name TEXT NOT NULL,
UNIQUE (bottle_id, name)
);"#;

const CREATE_DIRECTORIES_TABLE: &str = r#"
CREATE TABLE directories (
id INTEGER PRIMARY KEY,
database_id INTEGER NOT NULL UNIQUE REFERENCES databases(id),
path TEXT NOT NULL
);"#;

pub struct Registry {
    connection: Connection,
}

impl Registry {
    pub fn new(path: PathBuf) -> rusqlite::Result<Self> {
        let connection = Connection::open(path)?;
        // Check if tables exist, if not create them
        if !connection.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='shelves';")?
            .exists([])? {
            Self::create_tables(&connection)?;
        }
        Ok(Self { connection })
    }

    fn create_tables(connection: &Connection) -> rusqlite::Result<()> {
        connection.execute_batch(&format!(
            "{}{}{}{}{}",
            CREATE_SHELVES_TABLE,
            CREATE_BUCKETS_TABLE,
            CREATE_BOTTLES_TABLE,
            CREATE_DATABASES_TABLE,
            CREATE_DIRECTORIES_TABLE
        ))?;
        Ok(())
    }

    fn create_shelf(&self, origin: &str) -> rusqlite::Result<i64> {
        self.connection.execute(
            "INSERT INTO shelves (origin) VALUES (?1)",
            &[origin],
        )?;
        // Create default bucket
        let shelf_id = self.connection.last_insert_rowid();
        self.connection.execute(
            "INSERT INTO buckets (shelf_id, name, persisted, quota, expires) VALUES (?1, 'default', 0, NULL, NULL)",
            &[&shelf_id],
        )?;
        // Insert bottles for "ls", "opfs", "cache"
        let bucket_id = self.connection.last_insert_rowid();
        for identifier in ["ls", "opfs", "cache"] {
            self.connection.execute(
                "INSERT INTO bottles (bucket_id, identifier, quota) VALUES (?1, ?2, NULL)",
                (bucket_id, identifier),
            )?;
        }
        Ok(self.connection.last_insert_rowid())
    }

    pub fn create_bucket(&self, shelf_id: i64, name: &str, persisted: bool, quota: Option<i64>, expires: Option<i64>) -> rusqlite::Result<i64> {
        self.connection.execute(
            "INSERT INTO buckets (shelf_id, name, persisted, quota, expires) VALUES (?1, ?2, ?3, ?4, ?5)",
            (shelf_id, name, persisted as i64, quota, expires),
        )?;
        let bucket_id = self.connection.last_insert_rowid();
        // Insert bottles for "ls", "opfs", "cache"
        for identifier in ["ls", "opfs", "cache"] {
            self.connection.execute(
                "INSERT INTO bottles (bucket_id, identifier, quota) VALUES (?1, ?2, NULL)",
                (bucket_id, identifier),
            )?;
        }
        Ok(bucket_id)
    }

    pub fn get_localstorage_dir(&self, origin: &str, bucket: Option<&str>) -> rusqlite::Result<PathBuf> {
        let bucket = bucket.unwrap_or("default");
        let path = self.connection.prepare(
            "SELECT d.path FROM directories d \
            JOIN databases db ON d.database_id = db.id \
            JOIN bottles b ON db.bottle_id = b.id \
            JOIN buckets bu ON b.bucket_id = bu.id\
            JOIN shelves s ON bu.shelf_id = s.id\
            WHERE s.origin = ?1 AND bu.name = ?2 AND b.identifier = 'ls'",
        )?.query_one([origin, bucket], |row| {
            let path: String = row.get(0)?;
            Ok(PathBuf::from(path))
        }).optional()?;
        match path {
            Some(p) => Ok(p),
            None => {
                Err(rusqlite::Error::QueryReturnedNoRows)
            }
        }
    }

    pub fn get_indexeddb_dir(&self, origin: &str, bucket: Option<&str>, db: &str) -> rusqlite::Result<PathBuf> {
        let bucket = bucket.unwrap_or("default");
        let path = self.connection.prepare(
            "SELECT d.path FROM directories d \
            JOIN databases db ON d.database_id = db.id \
            JOIN bottles b ON db.bottle_id = b.id \
            JOIN buckets bu ON b.bucket_id = bu.id\
            JOIN shelves s ON bu.shelf_id = s.id\
            WHERE s.origin = ?1 AND bu.name = ?2 AND b.identifier = 'idb' AND db.name = ?3",
        )?.query_one([origin, bucket, db], |row| {
            let path: String = row.get(0)?;
            Ok(PathBuf::from(path))
        }).optional()?;
        match path {
            Some(p) => Ok(p),
            None => {
                Err(rusqlite::Error::QueryReturnedNoRows)
            }
        }
    }

    pub fn create_indexeddb_dir(&self, origin: &str, bucket: Option<&str>, db: &str) -> rusqlite::Result<PathBuf> {
        let bucket = bucket.unwrap_or("default");
        // Get shelf id
        let shelf_id: i64 = match self.connection.prepare(
            "SELECT id FROM shelves WHERE origin = ?1",
        )?.query_row([origin], |row| row.get(0)).optional()? {
            Some(id) => id,
            None => self.create_shelf(origin)?,
        };
        // Get bucket id
        let bucket_id: i64 = match self.connection.prepare(
            "SELECT id FROM buckets WHERE shelf_id = ?1 AND name = ?2",
        )?.query_row((shelf_id, bucket), |row| row.get(0)).optional()? {
            Some(id) => id,
            None => self.create_bucket(shelf_id, bucket, false, None, None)?,
        };
        // Create bottle if not exists
        let bottle_id: i64 = match self.connection.prepare(
            "SELECT id FROM bottles WHERE bucket_id = ?1 AND identifier = 'idb'",
        )?.query_row([bucket_id], |row| row.get(0)).optional()? {
            Some(id) => id,
            None => {
                self.connection.execute(
                    "INSERT INTO bottles (bucket_id, identifier, quota) VALUES (?1, 'idb', NULL)",
                    (bucket_id,),
                )?;
                self.connection.last_insert_rowid()
            },
        };
        // Create database
        self.connection.execute(
            "INSERT INTO databases (bottle_id, name) VALUES (?1, ?2)",
            (bottle_id, db),
        )?;
        let database_id = self.connection.last_insert_rowid();
        // Create directory path
        let dir_path = format!("storage/{}/{}/{}", origin, bucket, db);
        self.connection.execute(
            "INSERT INTO directories (database_id, path) VALUES (?1, ?2)",
            (database_id, dir_path.as_str()),
        )?;
        Ok(PathBuf::from(dir_path))
    }
}