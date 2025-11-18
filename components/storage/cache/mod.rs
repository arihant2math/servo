use std::collections::HashMap;
use std::path::PathBuf;
use std::thread;
use rusqlite::Connection;
use base::generic_channel;
use base::generic_channel::{GenericReceiver, GenericSender};
use servo_url::ImmutableOrigin;
use storage_traits::cache_thread::CacheThreadMsg;

pub trait CacheThreadFactory {
    fn new(config_dir: Option<PathBuf>) -> Self;
}

impl CacheThreadFactory for GenericSender<CacheThreadMsg> {
    /// Create a storage thread
    fn new(
        config_dir: Option<PathBuf>,
    ) -> GenericSender<CacheThreadMsg> {
        let (chan, port) = generic_channel::channel().unwrap();
        thread::Builder::new()
            .name("WebStorageManager".to_owned())
            .spawn(move || {
                CacheManager::new(port, config_dir).start();
            })
            .expect("Thread spawning failed");
        chan
    }
}

struct CacheManager {
    port: GenericReceiver<CacheThreadMsg>,
    config_dir: Option<PathBuf>,
    connections: HashMap<ImmutableOrigin, Connection>,
}

impl CacheManager {
    fn new(
        port: GenericReceiver<CacheThreadMsg>,
        config_dir: Option<PathBuf>,
    ) -> Self {
        CacheManager {
            port,
            connections: HashMap::new(),
            config_dir,
        }
    }

    fn start(&self) {
        while let Ok(msg) = self.port.recv() {
            match msg {
                CacheThreadMsg::HasStore(_, _, _) => {}
                CacheThreadMsg::DeleteStore(_, _, _) => {}
                CacheThreadMsg::StoreKeys(_, _) => {}
            }
        }
    }
}
