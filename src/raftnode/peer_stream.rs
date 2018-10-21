use tokio::io::ReadHalf;
use tokio::net::TcpStream;
use tokio::io::WriteHalf;
use bytes::BytesMut;
use raftnode::protocol::ProtocolMessage;
use futures::Future;
use futures::Async;
use raftnode::protocol::Protocol;
use bytes::BufMut;
use tokio::io::AsyncRead;
use futures::Stream;
use tokio::io::ErrorKind::WouldBlock;

pub struct PeerStream {
    connection_read : ReadHalf<TcpStream>,
    connection_write : WriteHalf<TcpStream>,
    read_buffer: BytesMut,
    tmp_read_buffer: [u8; 4096],
    messages: Vec<ProtocolMessage>
}

impl PeerStream {
    pub fn new(tcp_stream : TcpStream) -> PeerStream {

        let (connection_read, connection_write) = tcp_stream.split();

        PeerStream {
            connection_read,
            connection_write,
            read_buffer: BytesMut::new(),
            tmp_read_buffer: [0; 4096],
            messages: vec![]
        }

    }
}

impl Future for PeerStream {

    type Item = ProtocolMessage;
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {

        loop {
            match self.connection_read.poll_read(&mut self.tmp_read_buffer) {
                Ok(Async::NotReady) => {
                    println!("read all from Peer");
                    break;
                },
                Ok(Async::Ready(size)) => {

                    println!("start to read {} bytes from Peer", size);

                    self.read_buffer.put_slice(&self.tmp_read_buffer[0..size]);
                },
                Err(ref e) if e.kind() == WouldBlock => {
                    println!("read from peer would block");
                }
                Err(e) => {
                    println!("peer has an error, teardown.");
                    return Err(())
                },
            }
        };

        match Protocol::decode(&mut self.read_buffer) {
            Err(_) => panic!("issue with decoding a package"),
            Ok(Some(p)) => {
                println!("stream got package");
                Ok(Async::Ready(p))
            }
            Ok(None) => {
                println!("stream has no more package.");
                Ok(Async::NotReady)
            }
        }

    }
}