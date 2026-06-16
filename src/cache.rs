use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct CacheEntry {
    pub md5: String,
    pub mtime: i64,
    pub etag: String,
    pub last_modified: String,
}

#[derive(Serialize, Deserialize)]
struct CacheFile {
    version: u32,
    entries: HashMap<String, CacheEntry>,
}

pub struct Cache {
    entries: HashMap<String, CacheEntry>,
    path: PathBuf,
    dirty: bool,
}

impl Cache {
    pub fn load(profile: &str) -> Self {
        let cache_dir = dirs::cache_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("bksync");
        std::fs::create_dir_all(&cache_dir).ok();
        let path = cache_dir.join(format!("{}.json", profile));

        let entries = std::fs::read_to_string(&path)
            .ok()
            .and_then(|content| serde_json::from_str::<CacheFile>(&content).ok())
            .map(|cf| cf.entries)
            .unwrap_or_default();

        Self { entries, path, dirty: false }
    }

    pub fn get(&self, key: &str) -> Option<&CacheEntry> {
        self.entries.get(key)
    }

    pub fn set(&mut self, key: String, entry: CacheEntry) {
        self.entries.insert(key, entry);
        self.dirty = true;
    }

    pub fn remove(&mut self, key: &str) {
        self.entries.remove(key);
        self.dirty = true;
    }

    pub fn save(&mut self) {
        if !self.dirty {
            return;
        }
        let cf = CacheFile {
            version: 1,
            entries: std::mem::take(&mut self.entries),
        };
        if let Ok(content) = serde_json::to_string(&cf) {
            let _ = std::fs::write(&self.path, &content);
        }
        self.entries = cf.entries;
        self.dirty = false;
    }
}