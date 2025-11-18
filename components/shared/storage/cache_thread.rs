use ipc_channel::ipc::IpcSender;
use serde::{Deserialize, Serialize};
use servo_url::ImmutableOrigin;

#[derive(Debug, Deserialize, Serialize)]
enum CacheError {
    QuotaExceeded,
}

/// Request operations on the cache data associated with a particular url
#[derive(Debug, Deserialize, Serialize)]
pub enum CacheThreadMsg {
    HasStore(IpcSender<bool>, ImmutableOrigin, String),
    DeleteStore(IpcSender<Result<(), ()>>, ImmutableOrigin, String),
    StoreKeys(IpcSender<Vec<String>>, ImmutableOrigin),
}
