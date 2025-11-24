use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, sleep};

static CONNECTED: AtomicUsize = AtomicUsize::new(0);
static FAILED: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <server:port> <num_connections>", args[0]);
        std::process::exit(1);
    }

    let target = &args[1];
    let num_conns: usize = args[2].parse().expect("Invalid number");

    println!("Connecting to {} with {} connections...", target, num_conns);

    // Stats reporter
    let stats_handle = tokio::spawn(async {
        loop {
            sleep(Duration::from_secs(1)).await;
            let conn = CONNECTED.load(Ordering::Relaxed);
            let fail = FAILED.load(Ordering::Relaxed);
            println!("Connected: {} | Failed: {}", conn, fail);
        }
    });

    // Spawn connections
    let mut handles = Vec::new();

    for i in 0..num_conns {
        let target = target.to_string();

        let handle = tokio::spawn(async move {
            match TcpStream::connect(&target).await {
                Ok(mut stream) => {
                    CONNECTED.fetch_add(1, Ordering::Relaxed);

                    // Keep connection alive with periodic ping
                    loop {
                        if stream.write_all(b"PING\n").await.is_err() {
                            break;
                        }

                        let mut buf = [0u8; 1024];
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break, // Connection closed
                            Ok(_) => {}              // Got response
                        }

                        sleep(Duration::from_secs(30)).await;
                    }

                    CONNECTED.fetch_sub(1, Ordering::Relaxed);
                }
                Err(_) => {
                    FAILED.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        handles.push(handle);

        // Rate limit connection attempts
        if i % 100 == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    }

    // Wait forever (connections stay open)
    stats_handle.await.unwrap();
}
