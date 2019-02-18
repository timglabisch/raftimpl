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
use std::ops::DerefMut;
use std::sync::RwLockWriteGuard;
use std::sync::atomic::{AtomicUsize, Ordering};

const PEER_UNIQUE : AtomicUsize  = AtomicUsize::new(0);

#[derive(Clone, PartialEq, Debug)]
pub enum PeerState {
    NoState,
    Connecting,
    Connected,
}

#[derive(Clone, Debug)]
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

    pub fn get_created_at(&self) -> &Option<SystemTime>
    {
        &self.created_at
    }

    pub fn get_connected_since(&self) -> &Option<SystemTime>
    {
        &self.connected_since
    }

    pub fn get_ping_requests(&self) -> u64
    {
        self.ping_requests
    }

    pub fn get_ping_responses(&self) -> u64
    {
        self.ping_responses
    }
}

pub struct PeerMetricsHelper(Arc<RwLock<PeerMetrics>>);

impl PeerMetricsHelper {

    pub fn clone(&self) -> Self {
        PeerMetricsHelper(self.0.clone())
    }

    pub fn get_copy(&self) -> PeerMetrics
    {
        self.get_inner().clone()
    }

    fn get_inner(&self) -> RwLockWriteGuard<PeerMetrics> {
        self.0.write().expect("could not get inner")
    }

    pub fn update_created_at(&self) {
        self.get_inner().created_at = Some(SystemTime::now());
    }

    pub fn update_connected_since(&self)
    {
        self.get_inner().connected_since = Some(SystemTime::now());
    }

    pub fn increment_ping_requests(&self)
    {
        self.get_inner().ping_requests += 1;
    }

    pub fn increment_ping_responses(&self)
    {
        self.get_inner().ping_responses += 1;
    }
}

pub struct PeerStateHelper(Arc<RwLock<PeerState>>);

pub struct Peer {
    ident: PeerIdent,
    raft_node_handle: RaftNodeHandle,
    channel_in_receiver: Receiver<PeerCommand>,
    channel_in_sender: Sender<PeerCommand>,
    peer_stream: PeerStream,
    state: PeerStateHelper,
    metrics: PeerMetricsHelper,
}

impl Peer {

    pub fn new(
        ident : PeerIdent,
        raft_node_handle: RaftNodeHandle,
        peer_stream : PeerStream
    ) -> Self {

        let (channel_in_sender, channel_in_receiver) = channel::<PeerCommand>(100);

        Peer {
            ident,
            peer_stream,
            channel_in_receiver,
            channel_in_sender,
            raft_node_handle,
            state: PeerStateHelper(Arc::new(RwLock::new(PeerState::NoState))),
            metrics: PeerMetricsHelper (Arc::new(RwLock::new(PeerMetrics::new())))
        }
    }

    pub fn get_id(&self) -> u64
    {
        self.ident.get_id()
    }

    pub fn handle(&self) -> PeerHandle {
        PeerHandle {
            ident: self.ident.clone(),
            sender: self.channel_in_sender.clone(),
            state: self.state.clone(),
            metrics: self.metrics.clone(),
        }
    }

    pub fn get_state(&self) -> PeerStateHelper {
        self.state.clone()
    }

    pub fn get_ident(&self) -> &PeerIdent {
        &self.ident
    }
}

#[derive(PartialEq, Clone)]
pub struct PeerIdent {
    id: u64,
    unique_identifier: usize
}

impl PeerIdent {
    pub fn new(id : u64) -> Self {

        let unique_idenfificator = PEER_UNIQUE.fetch_add(1, Ordering::SeqCst);

        PeerIdent {
            id: p.get_id(),
            unique_identifier
        }
    }

    pub fn new_anon() -> Self
    {
        PeerIdent {
            id: 0,
            unique_identifier: 0
        }
    }

    pub fn is_anon(&self) -> bool {
        self.id == 0 && self.unique_identifier == 0
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_unique_identifier(&self) -> usize
    {
        self.unique_identifier
    }
}



impl Future for Peer {
    type Item = PeerIdent;
    type Error = PeerIdent;

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {

        loop {
            match self.channel_in_receiver.poll() {
                Ok(Async::NotReady) => {
                    println!("node {} | peer {} | no command from node from peer", self.raft_node_handle.get_id(), self.ident.get_id());
                    break;
                },
                Ok(Async::Ready(None)) => {
                    // just go on
                },
                Ok(Async::Ready(Some(ref command))) => {
                    println!("node {} | peer {} | got command {:?}", self.raft_node_handle.get_id(), self.ident.get_id(), command);

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
                                    self.metrics.increment_ping_requests();
                                },
                                ProtocolMessage::PingReponse(p) => {
                                    println!("got ping response");
                                    self.metrics.increment_ping_responses();
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
                    println!("node {} | peer {} | has an error, teardown.", self.raft_node_handle.get_id(), self.ident.get_id());
                    return Err(self.ident.clone())
                },
            }
        }

        loop {
            match self.peer_stream.poll() {
                Ok(Async::NotReady) => {
                    println!("node {} | peer {} | read all from Peer", self.raft_node_handle.get_id(), self.ident.get_id());
                    break;
                },
                Ok(Async::Ready(message)) => {
                    println!("node {} | peer {} | got message {:?}", self.raft_node_handle.get_id(), self.ident.get_id(), &message);
                    if self.channel_in_sender.try_send(PeerCommand::IncomingMessage(message)).is_err() {
                        println!("node {} | peer {} | peer has a full incoming queue, kill it.", self.raft_node_handle.get_id(), self.ident.get_id());
                        return Err(self.ident.clone());
                    }
                },
                Err(e) => {
                    println!("node {} | peer {} | peer has an error, teardown.", self.raft_node_handle.get_id(), self.ident.get_id());
                    return Err(self.ident.clone())
                },
            }
        };


        //println!("node {} | peer {} | peer done", self.raft_node_handle.get_id(), self.id);

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
    ident: PeerIdent,
    sender: Sender<PeerCommand>,
    state: PeerStateHelper,
    metrics: PeerMetricsHelper
}

impl PeerHandle {
    pub fn clone(&self) -> PeerHandle {
        PeerHandle {
            ident: self.ident.clone(),
            sender: self.sender.clone(),
            state: self.state.clone(),
            metrics: self.metrics.clone()
        }
    }

    pub fn send(&mut self, command: PeerCommand) -> Result<(), ()> {
        self.sender.try_send(command).map_err(|_| ())
    }

    pub fn get_state(&self) -> PeerStateHelper {
        self.state.clone()
    }

    pub fn get_metrics(&self) -> PeerMetricsHelper {
        self.metrics.clone()
    }

    pub fn get_ident(&self) -> &PeerIdent {
        &self.ident
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