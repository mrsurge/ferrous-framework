use std::{env, io, net::TcpListener, thread, time::Duration};

fn main() -> io::Result<()> {
    let port = env::args()
        .nth(1)
        .and_then(|raw| raw.parse::<u16>().ok())
        .unwrap_or(0);
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    listener.set_nonblocking(true)?;
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        match listener.accept() {
            Ok((_stream, _addr)) => return Ok(()),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}
