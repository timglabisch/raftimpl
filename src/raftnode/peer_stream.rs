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
use futures::Poll;
use tokio::io;
use tokio::io::AsyncWrite;
use tokio::prelude::future::poll_fn;

pub struct PeerStream {
    node_id: u64,
    address: String,
    tcp_stream: TcpStream,
    write_buffer: BytesMut,
    read_buffer: BytesMut,
    tmp_read_buffer: [u8; 4096],
}

impl PeerStream {

    pub fn new(node_id: u64, tcp_stream: TcpStream) -> PeerStream {
        let address = tcp_stream.peer_addr().expect("could not get peer_addr").to_string();

        //let (connection_read, connection_write) = tcp_stream.split();

        PeerStream {
            node_id,
            address,
            tcp_stream,
            read_buffer: BytesMut::new(),
            write_buffer: BytesMut::new(),
            tmp_read_buffer: [0; 4096],
        }
    }

    pub fn get_address(&self) -> &str {
        &self.address
    }

    pub fn add_to_write_buffer(&mut self, buf: &[u8])
    {
        self.write_buffer.put_slice(buf);
    }

    fn poll_flush(&mut self) -> Poll<(), io::Error> {
        // As long as there is buffered data to write, try to write it.
        while !self.write_buffer.is_empty() {
            // Try to write some bytes to the socket
            let n = try_ready!(self.tcp_stream.poll_write(&self.write_buffer));

            // As long as the wr is not empty, a successful write should
            // never write 0 bytes.
            assert!(n > 0);

            println!("node {} | sending {} bytes", self.node_id, n);

            // This discards the first `n` bytes of the buffer.
            let _ = self.write_buffer.split_to(n);
        }

        Ok(Async::Ready(()))
    }

    pub fn with_hello_message(mut self, message: ProtocolMessage) -> Self {
        let mut buf = BytesMut::new();

        match Protocol::encode(&message, &mut buf) {
            Ok(_) => {}
            Err(e) => { panic!("error on message encoding") }
        };

        self.write_buffer.put_slice(&buf);

        self
    }

    pub fn get_node_id(&self) -> u64 {
        self.node_id
    }

    pub fn poll(&mut self) -> Result<Async<ProtocolMessage>, ()> {
        loop {
            match self.poll_flush() {
                Ok(Async::Ready(_)) => {
                    println!("node {} | send queue empty, go on", self.node_id);
                    break;
                }
                Ok(Async::NotReady) => {
                    println!("node {} | send queue not empty, next round...", self.node_id);
                    return Ok(Async::NotReady);
                }
                Err(ref e) if e.kind() == WouldBlock => {
                    println!("node {} | write from peer would block", self.node_id);
                }
                Err(e) => {
                    println!("node {} | issue with the send buffer", self.node_id);
                    return Err(());
                }
            };
        }

        loop {

            self.read_buffer.reserve(1024);

            match self.tcp_stream.read_buf(&mut self.read_buffer) {
                Ok(Async::NotReady) => {
                    println!("node {} | read all from Peer", self.node_id);
                    break;
                }
                Ok(Async::Ready(size)) => {
                    println!("node {} | start to read {} bytes from Peer", self.node_id, size);

                    if size == 0 { // socket is closed?
                        println!("node {} | peer socket is closed", self.node_id);
                        return Err(());
                    }

                    // self.read_buffer.put_slice(&self.tmp_read_buffer[0..size]);

                }
                Err(ref e) if e.kind() == WouldBlock => {
                    println!("node {} | read from peer would block", self.node_id);
                }
                Err(e) => {
                    println!("node {} | peer has an error, teardown. {:?}", self.node_id, e);
                    return Err(());
                }
            }
        };

        match Protocol::decode(self.get_node_id(), &mut self.read_buffer) {
            Err(_) => panic!("issue with decoding a package"),
            Ok(Some(p)) => {
                println!("node {} | stream got package", self.node_id);
                Ok(Async::Ready(p))
            }
            Ok(None) => {
                println!("node {} | stream has no more package.", self.node_id);
                Ok(Async::NotReady)
            }
        }
    }
}