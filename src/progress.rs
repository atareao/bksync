use std::sync::atomic::{AtomicU64, Ordering};

use indicatif::{ProgressBar, ProgressStyle};

pub struct Progress {
    pub pb: ProgressBar,
    pub current: AtomicU64,
    pub downloaded: AtomicU64,
    pub uploaded: AtomicU64,
    pub deleted: AtomicU64,
}

impl Progress {
    pub fn new() -> Self {
        let pb = ProgressBar::new(0);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("##-"),
        );
        Self {
            pb,
            current: AtomicU64::new(0),
            downloaded: AtomicU64::new(0),
            uploaded: AtomicU64::new(0),
            deleted: AtomicU64::new(0),
        }
    }

    pub fn set_total(&self, total: u64) {
        self.pb.set_length(total);
    }

    pub fn start_operation(&self, label: &str, detail: &str) {
        self.current.store(0, Ordering::Relaxed);
        self.downloaded.store(0, Ordering::Relaxed);
        self.uploaded.store(0, Ordering::Relaxed);
        self.deleted.store(0, Ordering::Relaxed);
        self.pb.reset();
        self.pb.set_message(format!("{} {}...", label, detail));
    }

    pub fn file_skipped(&self, path: &str) {
        let pos = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.pb.set_position(pos);
        self.pb.set_message(format!("[ ] {}", path));
    }

    pub fn file_downloaded(&self, key: &str) {
        let pos = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.downloaded.fetch_add(1, Ordering::Relaxed);
        self.pb.set_position(pos);
        self.pb.set_message(format!("[v] {}", key));
    }

    pub fn file_uploaded(&self, key: &str) {
        let pos = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.uploaded.fetch_add(1, Ordering::Relaxed);
        self.pb.set_position(pos);
        self.pb.set_message(format!("[^] {}", key));
    }

    pub fn file_deleted(&self, path: &str) {
        let pos = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.deleted.fetch_add(1, Ordering::Relaxed);
        self.pb.set_position(pos);
        self.pb.set_message(format!("[x] {}", path));
    }

    pub fn summary(&self, label: &str) {
        let downloaded = self.downloaded.load(Ordering::Relaxed);
        let uploaded = self.uploaded.load(Ordering::Relaxed);
        let deleted = self.deleted.load(Ordering::Relaxed);

        let mut parts = Vec::new();
        if downloaded > 0 {
            parts.push(format!("{} descargados", downloaded));
        }
        if uploaded > 0 {
            parts.push(format!("{} subidos", uploaded));
        }
        if deleted > 0 {
            parts.push(format!("{} eliminados", deleted));
        }

        let detail = if parts.is_empty() {
            "sin cambios".to_string()
        } else {
            parts.join(", ")
        };

        self.pb.finish_with_message(format!("{} -- {}", label, detail));
    }
}