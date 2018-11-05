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
use raftnode::peer_stream::PeerStream;
use futures::Stream;

pub struct Peer {
    id : u64,
    raft_node_handle: RaftNodeHandle,
    mailbox_incoming: Vec<ProtocolMessage>,
    channel_in_receiver: Receiver<PeerCommand>,
    channel_in_sender: Sender<PeerCommand>,
    peer_stream: PeerStream
}

impl Peer {
    pub fn new(
        id : u64,
        raft_node_handle: RaftNodeHandle,
        peer_stream : PeerStream
    ) -> Self {

        let (channel_in_sender, channel_in_receiver) = channel::<PeerCommand>(100);

        Peer {
            id,
            peer_stream,
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
            match self.channel_in_receiver.poll() {
                Ok(Async::NotReady) => {
                    println!("no command from node from peer {}", self.id);
                    break;
                },
                Ok(Async::Ready(command)) => {
                    println!("got command {:?} from peer {}", command, self.id);
                },
                Err(e) => {
                    println!("peer {} has an error, teardown.", self.id);
                    return Err(())
                },
            }
        }

        loop {
            match self.peer_stream.poll() {
                Ok(Async::NotReady) => {
                    println!("read all from Peer 1 {}", self.id);
                    break;
                },
                Ok(Async::Ready(message)) => {
                    println!("read message from Peer {}", self.id);
                    self.mailbox_incoming.push(message);
                },
                Err(e) => {
                    println!("peer {} has an error, teardown.", self.id);
                    return Err(())
                },
            }
        };


        println!("client mailbox has {} items", self.mailbox_incoming.len());

        return Ok(Async::NotReady);
    }
}

#[derive(Debug)]
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

    pub fn send(&mut self, command: PeerCommand) {
        self.sender.try_send(command).expect("could not send to handle.");
    }
}
