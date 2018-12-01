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
use protos::hello::HelloResponse;
use protos::hello::PingResponse;
use std::sync::Arc;
use std::sync::RwLock;
use std::clone::Clone;
use std::time::SystemTime;

#[derive(Clone, PartialEq)]
pub enum PeerState {
    NoState,
    Connecting,
    Connected,
}

pub struct PeerMetrics {
    created_at: Option<SystemTime>,
    connected_since: Option<SystemTime>,
    ping_requests: u64,
    ping_responses: u64
}

impl PeerMetrics {
    pub fn new() -> Self {
        PeerMetrics {
            created_at: None,
            connected_since: None,
            ping_requests: 0,
            ping_responses: 0
        }
    }
}

pub struct PeerMetricsHelper(Arc<RwLock<PeerMetrics>>);

pub struct PeerStateHelper(Arc<RwLock<PeerState>>);

pub struct Peer {
    id : u64,
    raft_node_handle: RaftNodeHandle,
    channel_in_receiver: Receiver<PeerCommand>,
    channel_in_sender: Sender<PeerCommand>,
    peer_stream: PeerStream,
    successful_ping_requests: u64,
    successful_ping_responses: u64,
    state: PeerStateHelper,
    metrics: PeerMetricsHelper,
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
            channel_in_receiver,
            channel_in_sender,
            raft_node_handle,
            successful_ping_requests: 0,
            successful_ping_responses: 0,
            state: PeerStateHelper(Arc::new(RwLock::new(PeerState::NoState))),
            metrics: PeerMetricsHelper (Arc::new(RwLock::new(PeerMetrics::new())))
        }
    }

    pub fn get_id(&self) -> u64
    {
        self.id
    }

    pub fn get_successful_ping_requests(&self) -> u64 {
        self.successful_ping_requests
    }

    pub fn get_successful_ping_responses(&self) -> u64 {
        self.successful_ping_responses
    }

    pub fn handle(&self) -> PeerHandle {
        PeerHandle {
            sender: self.channel_in_sender.clone(),
            state: self.state.clone()
        }
    }

    pub fn get_state(&self) -> PeerStateHelper {
        self.state.clone()
    }
}

impl Future for Peer {
    type Item = u64;
    type Error = u64;

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {

        println!("start working on peer with {} successful_ping_responses", self.successful_ping_responses);

        loop {
            match self.channel_in_receiver.poll() {
                Ok(Async::NotReady) => {
                    println!("node {} | peer {} | no command from node from peer", self.raft_node_handle.get_id(), self.id);
                    break;
                },
                Ok(Async::Ready(None)) => {
                    // just go on
                },
                Ok(Async::Ready(Some(ref command))) => {
                    println!("node {} | peer {} | got command {:?}", self.raft_node_handle.get_id(), self.id, command);

                    match command {
                        &PeerCommand::IncomingMessage(ref message) => {
                            match message {
                                ProtocolMessage::PingRequest(p) => {
                                    let mut response = PingResponse::new();
                                    response.set_request_node_id(self.raft_node_handle.get_id());
                                    response.set_response_node_id(p.get_request_node_id());

                                    let mut bytes = BytesMut::new();
                                    Protocol::encode(&ProtocolMessage::PingReponse(response), &mut bytes)
                                        .expect("could not encode msg");

                                    self.peer_stream.add_to_write_buffer(&bytes);
                                    self.successful_ping_requests += 1;
                                },
                                ProtocolMessage::PingReponse(p) => {
                                    println!("got ping response");
                                    self.successful_ping_responses += 1;
                                }
                                _ => {
                                    panic!("incoming message type not implemented: {:?}", message);
                                }
                            }
                        },
                        &PeerCommand::OutgoingMessage(ref message) => {

                            let mut bytes = BytesMut::new();
                            Protocol::encode(message, &mut bytes)
                                .expect("could not encode msg");

                            println!("sending message to peer!");

                            self.peer_stream.add_to_write_buffer(&bytes);
                        },
                        _ => {
                            panic!("peer command not implemented");
                        }
                    };
                },
                Err(e) => {
                    println!("node {} | peer {} | has an error, teardown.", self.raft_node_handle.get_id(), self.id);
                    return Err(self.id)
                },
            }
        }

        loop {
            match self.peer_stream.poll() {
                Ok(Async::NotReady) => {
                    println!("node {} | peer {} | read all from Peer", self.raft_node_handle.get_id(), self.id);
                    break;
                },
                Ok(Async::Ready(message)) => {
                    println!("node {} | peer {} | got message {:?}", self.raft_node_handle.get_id(), self.id, &message);
                    if self.channel_in_sender.try_send(PeerCommand::IncomingMessage(message)).is_err() {
                        println!("node {} | peer {} | peer has a full incoming queue, kill it.", self.raft_node_handle.get_id(), self.id);
                        return Err(self.id);
                    }
                },
                Err(e) => {
                    println!("node {} | peer {} | peer has an error, teardown.", self.raft_node_handle.get_id(), self.id);
                    return Err(self.id)
                },
            }
        };


        println!("node {} | peer {} | peer done", self.raft_node_handle.get_id(), self.id);

        return Ok(Async::NotReady);
    }
}

#[derive(Debug)]
pub enum PeerCommand {
    IncomingMessage(ProtocolMessage),
    OutgoingMessage(ProtocolMessage)
}

pub struct PeerHandle
{
    sender: Sender<PeerCommand>,
    state: PeerStateHelper
}

impl PeerHandle {
    pub fn clone(&self) -> PeerHandle {
        PeerHandle {
            sender: self.sender.clone(),
            state: self.state.clone()
        }
    }

    pub fn send(&mut self, command: PeerCommand) -> Result<(), ()> {
        self.sender.try_send(command).map_err(|_| ())
    }

    pub fn get_state(&self) -> PeerStateHelper {
        self.state.clone()
    }
}


impl PeerStateHelper {
    pub fn get_state(&self) -> PeerState {
        self.0.read().expect("could not get read lock").clone()
    }

    pub fn clone(&self) -> Self {
        PeerStateHelper(self.0.clone())
    }

    pub fn has_no_state(&self) -> bool {
        self.get_state() == PeerState::NoState
    }

    pub fn mark_as(&self, state : PeerState) {
        *self.0.write().expect("could not get read lock") = state
    }
}