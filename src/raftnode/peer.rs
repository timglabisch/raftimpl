use tokio::net::TcpStream;
use futures::Future;
use futures::Async;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::io::AsyncRead;
use tokio::io::ErrorKind::WouldBlock;
use raftnode::protocol::ProtocolMessage;
use raftnode::protocol::Protocol;
use bytes::BytesMut;
use bytes::BufMut;

pub struct Peer {
    id : u64,
    connection_read : ReadHalf<TcpStream>,
    connection_write : WriteHalf<TcpStream>,
    read_buffer: BytesMut,
    tmp_read_buffer: [u8; 4096],
    mailbox_incoming: Vec<ProtocolMessage>
}

impl Peer {
    pub fn new(id : u64, connection : TcpStream) -> Self {

        let (connection_read, connection_write) = connection.split();

        Peer {
            id,
            connection_read,
            connection_write,
            read_buffer: BytesMut::new(),
            tmp_read_buffer: [0; 4096],
            mailbox_incoming: vec![]
        }
    }
}

impl Future for Peer {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {

        loop {
            match self.connection_read.poll_read(&mut self.tmp_read_buffer) {
                Ok(Async::NotReady) => {
                    println!("read all from Peer {}", self.id);
                },
                Ok(Async::Ready(size)) => {

                    println!("start to read {} bytes from Peer {}", size, self.id);

                    self.read_buffer.put_slice(&self.tmp_read_buffer[0..size]);

                    panic!("ok no...");
                },
                Err(ref e) if e.kind() == WouldBlock => {
                    println!("read from peer {} would block", self.id);
                }
                Err(e) => {
                    println!("peer {} has an error, teardown.", self.id);
                    return Err(())
                },
            }
        };

        loop {
            match Protocol::decode(&mut self.read_buffer) {
                Err(_) => panic!("issue with decoding a package"),
                Ok(Some(p)) => {
                    println!("decoded package.");
                    self.mailbox_incoming.push(p);
                }
                Ok(None) => {
                    println!("could not decode package - bytes missing, so go on.");
                    break;
                }
            }
        }

        println!("client mailbox has {} items", self.mailbox_incoming.len());

        return Ok(Async::NotReady);
    }
}