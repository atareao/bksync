use std::sync::atomic::{AtomicU64, Ordering};

use indicatif::{ProgressBar, ProgressStyle};

pub struct Progress {
    pub pb: ProgressBar,
    pub current: AtomicU64,
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
        }
    }

    pub fn set_total(&self, total: u64) {
        self.pb.set_length(total);
    }

    pub fn start_operation(&self, label: &str, detail: &str) {
        self.current.store(0, Ordering::Relaxed);
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
        self.pb.set_position(pos);
        self.pb.set_message(format!("[v] {}", key));
    }

    pub fn file_uploaded(&self, key: &str) {
        let pos = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.pb.set_position(pos);
        self.pb.set_message(format!("[^] {}", key));
    }

    pub fn file_deleted(&self, path: &str) {
        let pos = self.current.fetch_add(1, Ordering::Relaxed) + 1;
        self.pb.set_position(pos);
        self.pb.set_message(format!("[x] {}", path));
    }

    pub fn summary(&self, label: &str) {
        self.pb.finish_with_message(format!(
            "{} -- {} files",
            label,
            self.current.load(Ordering::Relaxed)
        ));
    }
}