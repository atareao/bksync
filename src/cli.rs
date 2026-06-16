use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "bksync", about = "S3 bucket sync CLI", version)]
pub struct Cli {
    #[arg(short = 'c', long = "config", default_value = "~/.config/bksync/config.toml")]
    pub config: String,

    #[arg(short = 'p', long = "profile", default_value = "default")]
    pub profile: String,

    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, help = "Verbose level: -v debug, -vv trace")]
    pub verbose: u8,

    #[arg(long = "dry-run", help = "Show what would be done without executing")]
    pub dry_run: bool,

    #[arg(long = "summary", help = "Show summary with download/upload counts at the end")]
    pub summary: bool,

    #[arg(long = "concurrency", help = "Max concurrent operations (overrides config)")]
    pub concurrency: Option<usize>,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(about = "Download from S3 bucket to local directory")]
    Pull {
        #[arg(long = "path", short = 'P', help = "S3 key prefix: single object or directory (e.g. docs/ or file.md)")]
        path: Option<String>,
        #[arg(long, help = "Include pattern (can be repeated)")]
        include: Vec<String>,
        #[arg(long, help = "Exclude pattern (can be repeated)")]
        exclude: Vec<String>,
        #[arg(long, help = "Delete local files not present in S3")]
        delete: bool,
    },
    #[command(about = "Upload from local directory to S3 bucket")]
    Push {
        #[arg(long = "path", short = 'P', help = "S3 key prefix: single object or directory (e.g. docs/ or file.md)")]
        path: Option<String>,
        #[arg(long, help = "Include pattern (can be repeated)")]
        include: Vec<String>,
        #[arg(long, help = "Exclude pattern (can be repeated)")]
        exclude: Vec<String>,
        #[arg(long, help = "Delete S3 objects not present locally")]
        delete: bool,
    },
    #[command(about = "Bidirectional sync between S3 and local directory")]
    Sync {
        #[arg(long = "path", short = 'P', help = "S3 key prefix: single object or directory (e.g. docs/ or file.md)")]
        path: Option<String>,
        #[arg(long, help = "Include pattern (can be repeated)")]
        include: Vec<String>,
        #[arg(long, help = "Exclude pattern (can be repeated)")]
        exclude: Vec<String>,
        #[arg(long, help = "Delete files/objects not present on the other side")]
        delete: bool,
    },
}