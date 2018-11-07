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
    connection_read : ReadHalf<TcpStream>,
    connection_write : WriteHalf<TcpStream>,
    write_buffer: BytesMut,
    read_buffer: BytesMut,
    tmp_read_buffer: [u8; 4096],
}

impl PeerStream {
    pub fn new(node_id: u64, tcp_stream : TcpStream) -> PeerStream {

        let (connection_read, connection_write) = tcp_stream.split();

        PeerStream {
            node_id,
            connection_read,
            connection_write,
            read_buffer: BytesMut::new(),
            write_buffer: BytesMut::new(),
            tmp_read_buffer: [0; 4096],
        }

    }

    fn poll_flush(&mut self) -> Poll<(), io::Error> {
        // As long as there is buffered data to write, try to write it.
        while !self.write_buffer.is_empty() {
            // Try to write some bytes to the socket
            let n = try_ready!(self.connection_write.poll_write(&self.write_buffer));

            // As long as the wr is not empty, a successful write should
            // never write 0 bytes.
            assert!(n > 0);

            println!("node {} | sending {} bytes", self.node_id, n);

            // This discards the first `n` bytes of the buffer.
            let _ = self.write_buffer.split_to(n);
        }

        Ok(Async::Ready(()))
    }

    pub fn with_hello_message(mut self, message : ProtocolMessage) -> Self {

        let mut buf =  BytesMut::new();

        match Protocol::encode(message, &mut buf) {
            Ok(_) => {},
            Err(e) => { panic!("error on message encoding") }
        };

        self.write_buffer.put_slice(&buf);

        self
    }

    pub fn get_node_id(&self) -> u64 {
        self.node_id
    }
}

impl Future for PeerStream {

    type Item = ProtocolMessage;
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {

        if self.poll_flush().is_err() {
            return Err(());
        }

        loop {
            match self.connection_read.poll_read(&mut self.tmp_read_buffer) {
                Ok(Async::NotReady) => {
                    println!("node {} | read all from Peer 3", self.node_id);
                    break;
                },
                Ok(Async::Ready(size)) => {

                    println!("node {} | start to read {} bytes from Peer", self.node_id, size);

                    self.read_buffer.put_slice(&self.tmp_read_buffer[0..size]);

                    if size == 0 {
                        break; // todo, is this correct?
                    }
                },
                Err(ref e) if e.kind() == WouldBlock => {
                    println!("node {} | read from peer would block", self.node_id);
                }
                Err(e) => {
                    println!("node {} | peer has an error, teardown.", self.node_id);
                    return Err(())
                },
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