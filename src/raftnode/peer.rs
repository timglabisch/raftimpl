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
use futures::sync::mpsc::{Receiver, Sender};
use futures::sync::mpsc::channel;
use raftnode::node::RaftNodeHandle;

pub struct Peer {
    id : u64,
    raft_node_handle: RaftNodeHandle,
    connection_read : ReadHalf<TcpStream>,
    connection_write : WriteHalf<TcpStream>,
    read_buffer: BytesMut,
    tmp_read_buffer: [u8; 4096],
    mailbox_incoming: Vec<ProtocolMessage>,
    channel_in_receiver: Receiver<PeerCommand>,
    channel_in_sender: Sender<PeerCommand>,
}

impl Peer {
    pub fn new(
        id : u64,
        raft_node_handle: RaftNodeHandle,
        connection : TcpStream
    ) -> Self {

        let (connection_read, connection_write) = connection.split();

        let (channel_in_sender, channel_in_receiver) = channel::<PeerCommand>(100);

        Peer {
            id,
            connection_read,
            connection_write,
            read_buffer: BytesMut::new(),
            tmp_read_buffer: [0; 4096],
            mailbox_incoming: vec![],
            channel_in_receiver,
            channel_in_sender,
            raft_node_handle
        }
    }

    pub fn handle(&self) -> PeerHandle {
        PeerHandle {
            sender: self.channel_in_sender.clone()
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
                    println!("no command from node from peer {}", self.id);
                },
                Ok(Async::Ready(command)) => {
                    println!("got command {:?} from peer {}", command, self.id);
                },
                Err(ref e) if e.kind() == WouldBlock => {
                    println!("got command would block {}", self.id);
                }
                Err(e) => {
                    println!("peer {} has an error, teardown.", self.id);
                    return Err(())
                },
            }
        }

        loop {
            match self.connection_read.poll_read(&mut self.tmp_read_buffer) {
                Ok(Async::NotReady) => {
                    println!("read all from Peer {}", self.id);
                },
                Ok(Async::Ready(size)) => {

                    println!("start to read {} bytes from Peer {}", size, self.id);

                    self.read_buffer.put_slice(&self.tmp_read_buffer[0..size]);
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


pub enum PeerCommand {

}

pub struct PeerHandle
{
    sender: Sender<PeerCommand>,
}

impl PeerHandle {
    pub fn clone(&self) -> PeerHandle {
        PeerHandle {
            sender: self.sender.clone()
        }
    }

    pub fn send(&self, command: PeerCommand) {
        self.send(command);
    }
}
