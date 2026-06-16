# bksync

> S3-compatible bidirectional sync CLI for local directories and object storage.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange)](https://www.rust-lang.org)
[![CI](https://img.shields.io/badge/CI-passing-brightgreen)](#)

`bksync` is a fast, concurrent CLI tool for synchronizing local directories with any S3-compatible object storage (AWS S3, MinIO, Garage, DigitalOcean Spaces, Backblaze B2, rustfs, etc.). It supports pull, push, bidirectional sync, and a persistent daemon mode with real-time file watching.

## Features

- **Pull** — download from S3 to local
- **Push** — upload from local to S3
- **Sync** — bidirectional, with timestamp-based conflict resolution
- **Daemon** — continuous sync with file system watching + periodic S3 refresh
- **Concurrent transfers** — configurable concurrency with semaphore-based limiting
- **Glob filtering** — include/exclude patterns per profile
- **Smart caching** — persistent JSON cache to skip unchanged files via MD5, etag, and mtime
- **S3-compatible** — works with any S3-compatible API (`force_path_style`, optional path normalization disable)
- **Key prefix** — scope operations to a subdirectory via `-P/--path`
- **Conflict files** — creates `<file>.conflict-<timestamp>` when both sides modify the same file
- **Progress bar** — real-time terminal feedback with per-file status
- **Systemd integration** — template unit file for running as a service

## Installation

### From source

```bash
git clone https://github.com/atareao/bksync.git
cd bksync
cargo build --release
cp target/release/bksync ~/.local/bin/
```

### Requirements

- Rust 1.85+
- An S3-compatible object storage endpoint

## Configuration

Create a config file at `~/.config/bksync/config.toml`:

```toml
[profile.default]
endpoint = "https://s3.example.com"
region = "us-east-1"
bucket = "my-bucket"
access_key = "YOUR_ACCESS_KEY"
secret_key = "YOUR_SECRET_KEY"
local_dir = "/home/user/s3sync"
include = ["**/*"]
exclude = [".DS_Store", "*.tmp", "*.log", ".git/"]
concurrency = 10
```

The config file is auto-created on first run with a template if it doesn't exist. Multiple profiles are supported:

```toml
[profile.production]
endpoint = "https://s3.amazonaws.com"
# ...
```

> **Note:** CLI flags `--include`/`--exclude` override the profile's patterns entirely, they don't merge.

## Usage

### Pull (S3 → Local)

```bash
bksync pull
bksync pull --path docs/ --delete
```

### Push (Local → S3)

```bash
bksync push
bksync push --path backups/ --include "*.tar.gz" --exclude "*.tmp"
```

### Bidirectional sync

```bash
bksync sync
bksync sync --delete --summary
```

### Daemon mode

```bash
bksync daemon
bksync daemon --refresh-minutes 30 --debounce-ms 1000
```

The daemon watches the local directory via `inotify` and polls S3 periodically. Conflicts are handled by creating `.conflict-<timestamp>` files.

### Global flags

| Flag | Description | Default |
|---|---|---|
| `-c, --config` | Path to config file | `~/.config/bksync/config.toml` |
| `-p, --profile` | Profile name | `default` |
| `-v` | Debug logging | info |
| `-vv` | Trace logging | info |
| `--dry-run` | Show actions without executing | `false` |
| `--summary` | Show download/upload/delete summary | `false` |
| `--concurrency` | Max concurrent transfers | `10` |

## How it works

### Sync logic

Each operation builds a set of keys (from S3, local, or both) and decides per-file:

| Condition | Action |
|---|---|
| Only in S3 | Download |
| Only in local | Upload |
| Both, local newer | Upload |
| Both, S3 newer | Download (conflict file created if local also modified) |
| Both, same mtime, same content | Skip |
| Both, same mtime, different content | Conflict: `.conflict-<ts>` + local wins |

### Cache

A JSON cache at `$XDG_CACHE_DIR/bksync/<profile>.json` stores MD5, etag, mtime, and last_modified per key. Files that haven't changed since the last sync are skipped without network I/O.

### Conflict resolution

When both sides modify the same file within the detection window, the daemon:
1. Renames the local file to `<key>.conflict-<timestamp>`
2. Downloads the S3 version
3. Logs a warning

This ensures no data is ever silently overwritten.

## Daemon as a systemd service

```bash
sudo cp systemd/bksync@.service /etc/systemd/system/
sudo systemctl enable --now bksync@default.service
journalctl -fu bksync@default.service
```

The template unit (`%i` → profile name) supports multiple profiles:

```bash
sudo systemctl enable --now bksync@production.service
```

## Development

```bash
cargo build
cargo test
cargo clippy
```

The project uses Rust edition 2024, `tokio` async runtime, and the official `aws-sdk-s3` v1 crate.

## License

MIT © 2026 Lorenzo Carbonell