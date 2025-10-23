/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::borrow::ToOwned;
use std::collections::HashMap;
use std::path::PathBuf;
use std::thread;
use ipc_channel::ipc;
use ipc_channel::ipc::IpcSender;
use ipc_channel::ipc::IpcReceiver;
use uuid::Uuid;
use storage_traits::storage_manager_thread::{ContainerIdent, CreateContainerError, StorageManagerThreadMsg};
use crate::storage_manager::registry::Registry;

pub trait StorageManagerThreadFactory {
    fn new(config_dir: Option<PathBuf>) -> Self;
}

impl StorageManagerThreadFactory for IpcSender<StorageManagerThreadMsg> {
    /// Create a storage thread
    fn new(
        config_dir: Option<PathBuf>,
    ) -> IpcSender<StorageManagerThreadMsg> {
        let (chan, port) = ipc::channel().unwrap();
        thread::Builder::new()
            .name("StorageManager".to_owned())
            .spawn(move || {
                    StorageManager::new(port, config_dir).start()
            })
            .expect("Thread spawning failed");
        chan
    }
}

struct StorageManager {
    port: IpcReceiver<StorageManagerThreadMsg>,
    storage_dir: PathBuf,
    registries: HashMap<ContainerIdent, Registry>,
}

impl StorageManager {
    fn new(
        port: IpcReceiver<StorageManagerThreadMsg>,
        config_dir: Option<PathBuf>,
    ) -> Self {
        let config_dir = match &config_dir {
            Some(dir) => dir.clone(),
            None => {
                PathBuf::new()
            }
        };
        let storage_dir = config_dir.join("storage");
        Self {
            port,
            storage_dir,
            registries: HashMap::new(),
        }
    }

    fn container_to_path(&self, ident: &ContainerIdent) -> PathBuf {
        const NAMESPACE_SERVO_STORAGE_MANAGER: &uuid::Uuid = &Uuid::from_bytes([
            0x37, 0x9e, 0x56, 0xb0, 0x1a, 0x76, 0x44, 0xc2, 0xa0, 0xdb, 0xe2, 0x18, 0xc5, 0xc8, 0xa3,
            0x5d,
        ]);


        let folder_name = match ident {
            ContainerIdent::Default => "default".to_owned(),
            // TODO: maybe hash the name
            ContainerIdent::Persistent(name) => format!("c-{}", name),
            ContainerIdent::Private(name) => {
                let uuid = Uuid::new_v5(NAMESPACE_SERVO_STORAGE_MANAGER, name.as_bytes());
                format!("{}", uuid.hyphenated())
            }
        };
        self.storage_dir.join(folder_name)
    }

    fn container_exists(&self, ident: &ContainerIdent) -> bool {
        let path = self.container_to_path(ident);
        path.exists()
    }

    fn create_container(&self, ident: &ContainerIdent) -> Result<Registry, CreateContainerError> {
        if self.container_exists(&ident) {
            return Err(CreateContainerError::ContainerExists);
        }
        let path = self.container_to_path(&ident);
        std::fs::create_dir_all(&path)
            .map_err(|e| CreateContainerError::Other(format!("Failed to create container directory: {}", e)))?;
        // Create registry
        let registry_path = path.join("registry.sqlite");
        Ok(Registry::new(registry_path)
            .map_err(|e| CreateContainerError::Other(format!("Failed to create registry: {}", e)))?)
    }

    /// A fallback to open an existing container if it hasn't been loaded into memory yet
    fn open_container(&self, ident: &ContainerIdent) -> Option<Registry> {
        if !self.container_exists(ident) {
            return None;
        }
        let path = self.container_to_path(ident);
        let registry_path = path.join("registry.sqlite");
        Registry::new(registry_path).ok()
    }

    fn get_registry(&mut self, ident: &ContainerIdent) -> Option<&mut Registry> {
        if !self.registries.contains_key(ident) {
            // TODO: This is slow
            // if let Some(registry) = self.open_container(ident) {
            //     self.registries.insert(ident.clone(), registry);
            // } else {
            //     return None;
            // }
            // But this is incorrect if a container is created
            return None;
        }
        self.registries.get_mut(ident)
    }

    fn delete_container(&mut self, ident: &ContainerIdent) -> Result<(), String> {
        if let Some(registry) = self.registries.remove(ident) {
            drop(registry);
        }
        let path = self.container_to_path(ident);
        std::fs::remove_dir_all(&path)
            .map_err(|e| format!("Failed to delete container directory: {}", e))?;
        if matches!(ident, ContainerIdent::Default) {
            // Recreate default container
            if let Ok(registry) = self.create_container(&ContainerIdent::Default) {
                self.registries.insert(ContainerIdent::Default, registry);
            }
        }
        Ok(())
    }

    fn start(&mut self) {
        // Create storage directory if it doesn't exist
        std::fs::create_dir_all(&self.storage_dir).expect("Failed to create storage directory");
        // Collect all folders in storage directory
        let mut container_paths = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.storage_dir) {
            for entry in entries {
                if let Ok(entry) = entry {
                    if entry.path().is_dir() {
                        container_paths.push(entry.path());
                    }
                }
            }
        }
        for path in container_paths {
            let registry_path = path.join("registry.sqlite");
            if let Ok(registry) = Registry::new(registry_path) {
                let folder_name = path.file_name().unwrap().to_string_lossy();
                let ident = if folder_name == "default" {
                    ContainerIdent::Default
                } else if folder_name.starts_with("c-") {
                    let name = folder_name[2..].to_owned();
                    ContainerIdent::Persistent(name)
                } else {
                    // Delete private containers on startup
                    let _ = std::fs::remove_dir_all(&path);
                    break;
                };
                self.registries.insert(ident, registry);
            }
        }
        if !self.registries.contains_key(&ContainerIdent::Default) {
            if let Ok(registry) = self.create_container(&ContainerIdent::Default) {
                self.registries.insert(ContainerIdent::Default, registry);
            }
        }
        loop {
            let Ok(msg) = self.port.recv() else {
                break;
            };
            match msg {
                StorageManagerThreadMsg::CreateContainer(ident, reply) => {
                    let result = self.create_container(&ident);
                    let result = result.map(|reg| {
                        self.registries.insert(ident, reg);
                    });
                    let _ = reply.send(result);
                }
                StorageManagerThreadMsg::DeleteContainer(ident, reply) => {
                    let result = self.delete_container(&ident);
                    let _ = reply.send(result);
                }
                StorageManagerThreadMsg::Exit(reply) => {
                    let _ = reply.send(());
                    break;
                }
            }
        }
    }
}

impl Drop for StorageManager {
    fn drop(&mut self) {
        let registries = std::mem::take(&mut self.registries);
        for (ident, registry) in registries {
            drop(registry);
            if let ContainerIdent::Private(_) = ident {
                let path = self.container_to_path(&ident);
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }
}
