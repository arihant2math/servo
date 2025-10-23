use ipc_channel::ipc::IpcSender;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ContainerIdent {
    Default,
    Persistent(String),
    Private(String)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum CreateContainerError {
    ContainerExists,
    Other(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum StorageManagerThreadMsg {
    /// Create a storage container
    CreateContainer(ContainerIdent, IpcSender<Result<(), CreateContainerError>>),
    /// Delete a storage container
    DeleteContainer(ContainerIdent, IpcSender<Result<(), String>>),

    /// Send a reply when done cleaning up thread resources and then shut it down
    Exit(IpcSender<()>),
}