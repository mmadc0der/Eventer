use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use eventer::Store;

#[derive(Debug, Parser)]
#[command(name = "eventer-server", about = "Serve an Eventer store over HTTP")]
struct Args {
    /// Directory for segment files. Created if missing.
    #[arg(long)]
    data: PathBuf,
    /// Path to the JSON schema file.
    #[arg(long)]
    schema: PathBuf,
    /// Listen address.
    #[arg(long, default_value = "127.0.0.1:43123")]
    bind: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let store = match Store::open(&args.data, &args.schema) {
        Ok(store) => Arc::new(store),
        Err(err) => {
            eprintln!("failed to open store: {err}");
            return ExitCode::from(1);
        }
    };
    println!("eventer listening on http://{}", args.bind);
    if let Err(err) = eventer_server::serve(Arc::clone(&store), &args.bind).await {
        eprintln!("server error: {err}");
        let _ = store.close();
        return ExitCode::from(1);
    }
    if let Err(err) = store.close() {
        eprintln!("close error: {err}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}
