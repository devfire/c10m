use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let target_addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let connection_count = 100_000; // Start with 10k, user can increase
    /*
    Here is the breakdown of why this specific combination is used:

    1. Arc<T> (Atomic Reference Counting)
    Problem: We are spawning 10,000 Tokio tasks. Each task needs access to these counters to update them.
    In Rust, a value has a single owner, so we can't just pass the variable itself to multiple tasks.
    Solution: Arc allows shared ownership. It puts the data on the heap and keeps a reference count.
    When we .clone() the Arc, we get a new handle to the same data, and the count goes up.
    When a task finishes and drops its handle, the count goes down.
    The data is only cleaned up when the count hits zero.

    Why "Atomic"?: Standard Rc is faster but not thread-safe.
    Since Tokio tasks can run on different threads, the reference counter itself must be atomic to ensure thread safety.

    2. AtomicUsize (Thread-Safe Integer)
    Problem: Even if multiple tasks can see the data (thanks to Arc), they can't safely mutate a standard integer (usize) at the same time. Doing so would cause a data race.
    Solution: AtomicUsize is a special integer type supported by the CPU hardware. It allows operations like "add 1" (fetch_add) to happen atomically—meaning the CPU guarantees the operation completes without interruption or interference from other threads.
    Why not Mutex<usize>?: we could use a Mutex, but it's much heavier.
    A Mutex requires locking, which can cause contention when 10,000 tasks try to update it simultaneously.
    Atomics are lock-free and extremely fast, making them perfect for high-performance counters like this.
    Summary of the Pattern
    Arc lets the data live as long as any task needs it.
    AtomicUsize lets multiple tasks update that data safely without locking.
    In our loop, we see active_connections.clone(). This creates a new handle for that specific task, allowing it to increment the counter when it connects and decrement it when it finishes.


     */
    let active_connections = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));

    println!(
        "Starting load test. Target: {}, Connections: {}",
        target_addr, connection_count
    );

    let mut handles = Vec::with_capacity(connection_count);

    for i in 0..connection_count {
        let active = active_connections.clone();
        let errors = total_errors.clone();

        // Small delay to avoid overwhelming the OS immediately (thundering herd on connect)
        if i % 100 == 0 {
            sleep(Duration::from_millis(1)).await;
        }

        handles.push(tokio::spawn(async move {
            match TcpStream::connect(target_addr).await {
                Ok(mut stream) => {
                    active.fetch_add(1, Ordering::Relaxed);
                    let mut buf = [0u8; 1024];
                    let msg = b"ping";

                    loop {
                        if let Err(e) = stream.write_all(msg).await {
                            eprintln!("Failed to write to socket: {}", e);
                            break;
                        }

                        if let Err(e) = stream.read(&mut buf).await {
                            eprintln!("Failed to read from socket: {}", e);
                            break;
                        }

                        // Keep connection alive, send heartbeat every second
                        sleep(Duration::from_secs(1)).await;
                    }
                    active.fetch_sub(1, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("Failed to connect to server: {}", e);
                    errors.fetch_add(1, Ordering::Relaxed);

                    // exit everything
                    std::process::exit(1);
                }
            }
        }));
    }

    // Monitor loop
    loop {
        let current = active_connections.load(Ordering::Relaxed);
        let errs = total_errors.load(Ordering::Relaxed);
        println!("Active Connections: {}, Total Errors: {}", current, errs);
        if current == 0 && errs == connection_count {
            println!("All connections failed.");
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}
