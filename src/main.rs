use mimalloc::MiMalloc;
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr: SocketAddr = "172.16.1.162:8080".parse().unwrap();

    // Create a socket2 socket for advanced configuration
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;

    // Set SO_REUSEADDR and SO_REUSEPORT (if available on target OS, usually Linux)
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;

    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;

    // Convert to Tokio TcpListener
    let listener = TcpListener::from_std(socket.into())?;

    println!("Listening on {}", addr);

    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Accept error: {}", e);
                continue;
            }
        };

        tokio::spawn(async move {
            // Disable Nagle's algorithm for lower latency
            if let Err(e) = socket.set_nodelay(true) {
                eprintln!("Failed to set TCP_NODELAY: {}", e);
            }

            let mut buf = [0u8; 1024];

            loop {
                match socket.read(&mut buf).await {
                    Ok(0) => return, // Connection closed
                    Ok(n) => {
                        if let Err(e) = socket.write_all(&buf[..n]).await {
                            eprintln!("Failed to write to socket: {}", e);
                            return;
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to read from socket: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        });
    }
}
