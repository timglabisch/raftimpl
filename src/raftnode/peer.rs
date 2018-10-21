use tokio::net::TcpStream;
use futures::Future;
use futures::Async;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::io::AsyncRead;
use tokio::io::ErrorKind::WouldBlock;

pub struct Peer {
    id : u64,
    connection_read : ReadHalf<TcpStream>,
    connection_write : WriteHalf<TcpStream>,
    read_buffer: Vec<u8>,
    tmp_read_buffer: [u8; 4096]
}

impl Peer {
    pub fn new(id : u64, connection : TcpStream) -> Self {

        let (connection_read, connection_write) = connection.split();

        Peer {
            id,
            connection_read,
            connection_write,
            read_buffer: vec![],
            tmp_read_buffer: [0; 4096]
        }
    }
}

impl Future for Peer {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        match self.connection_read.poll_read(&mut self.tmp_read_buffer) {
            Ok(Async::NotReady) => {

                println!("we got something, and there is more! :)");

                Ok(Async::NotReady)
            },
            Ok(Async::Ready(_)) => {
                panic!("ok no...");
            },
            Err(ref e) if e.kind() == WouldBlock => {
                Ok(Async::NotReady)
            }
            Err(e) => {
                println!("peer err");
                Err(())
            },
        }
    }
}