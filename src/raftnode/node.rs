use raft::storage::MemStorage;
use raft::Config;
use raft::RawNode;
use futures::Future;
use futures::Async;
use tokio::net::TcpListener;
use futures::prelude::*;
use tokio::io::copy;
use tokio::io::AsyncRead;
use tokio;
use raftnode::peer::Peer;
use tokio::timer::Interval;
use std::time::Duration;
use tokio::prelude::StreamExt;
use std::time::Instant;
use tokio::io::ErrorKind::WouldBlock;
use futures::sync::mpsc::{Receiver, Sender};
use futures::sync::mpsc::channel;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::ops::Deref;
use std::sync::RwLock;
use raftnode::peer::PeerHandle;
use std::collections::HashMap;
use futures::Sink;
use tokio::net::TcpStream;
use raftnode::peer_stream::PeerStream;
use raftnode::peer_inflight::PeerInflight;
use raftnode::protocol::ProtocolMessage;
use protos::hello::HelloRequest;
use futures::future::Either;
use raftnode::node_peer_slot::PeerSlotMap;
use raftnode::node_peer_slot::NodePeerSlot;

pub struct RaftNode {
    peer_counter: Arc<AtomicUsize>,
    // every new connection (aka. peer) will get a new id.
    config: Config,
    raw_node: RawNode<MemStorage>,
    tcp_server: Option<Box<Future<Item=(), Error=()> + Send>>,
    interval: Interval,
    channel_in_receiver: Receiver<RaftNodeCommand>,
    channel_in_sender: Sender<RaftNodeCommand>,
    peers: Arc<RwLock<PeerSlotMap>>,
}

pub struct RaftNodePeerInfo {
    id: u64,
    handle: PeerHandle,
}

impl RaftNode {
    pub fn handle(&self) -> RaftNodeHandle {
        RaftNodeHandle {
            id: self.config.id,
            sender: self.channel_in_sender.clone(),
        }
    }

    pub fn new(node_id: u64) -> Self {
        let storage = MemStorage::new();

        let config = Config {
            // The unique ID for the Raft node.
            id: node_id,
            // The Raft node list.
            // Mostly, the peers need to be saved in the storage
            // and we can get them from the Storage::initial_state function, so here
            // you need to set it empty.
            peers: vec![1],
            // Election tick is for how long the follower may campaign again after
            // it doesn't receive any message from the leader.
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
            max_size_per_msg: 1024 * 1024 * 1024,
            // Max inflight msgs that the leader sends messages to follower without
            // receiving ACKs.
            max_inflight_msgs: 256,
            // The Raft applied index.
            // You need to save your applied index when you apply the committed Raft logs.
            applied: 0,
            // Just for log
            tag: format!("[{}]", 1),
            ..Default::default()
        };

        let raw_node = RawNode::new(&config, storage, vec![]).unwrap();

        let node_id = config.id;

        let (channel_in_sender, channel_in_receiver) = channel::<RaftNodeCommand>(100);

        let mut peer_slot_map = PeerSlotMap::new();
        peer_slot_map.insert(NodePeerSlot::new(1, "127.0.0.1:2001".into(), None));
        peer_slot_map.insert(NodePeerSlot::new(2, "127.0.0.1:2002".into(), None));
        // peer_slot_map.insert(NodePeerSlot::new(3, "127.0.0.1:2003".into(), None));

        RaftNode {
            peer_counter: Arc::new(AtomicUsize::new(0)),
            config,
            raw_node,
            tcp_server: None,
            interval: Interval::new_interval(Duration::from_millis(1000)),
            channel_in_receiver,
            channel_in_sender,
            peers: Arc::new(RwLock::new(peer_slot_map)),
        }
    }

    pub fn listen(&mut self) -> () {
        if self.tcp_server.is_some() {
            return ();
        }

        let port = format!("127.0.0.1:200{}", self.config.id);

        println!("node {} | running on port {}", self.config.id, port);

        let addr = port.parse().unwrap();

        let listener = TcpListener::bind(&addr)
            .expect("unable to bind TCP listener");

        let raft_node_handle = self.handle().clone();


        let peer_counter = self.peer_counter.clone();
        let peer_map = self.peers.clone();
        let node_id = self.config.id;

        let server = listener.incoming()
            .map_err(|e| eprintln!("accept failed = {:?}", e))
            .for_each(move |tcp_stream| {

                let mut hello_request = HelloRequest::new();
                hello_request.set_node_id(node_id);

                let stream = PeerStream::new(node_id, tcp_stream)
                    .with_hello_message(ProtocolMessage::Hello(hello_request));

                let raft_node_handle = raft_node_handle.clone();
                let peer_map = peer_map.clone();

                PeerInflight::new(stream)
                    .and_then(move |(peer_id, peer_stream)|{

                    let address = peer_stream.get_address().to_string();

                    let peer = Peer::new(
                        peer_id,
                        raft_node_handle.clone(),
                        peer_stream,
                    );

                    {
                        let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");

                        peer_map.insert(
                            NodePeerSlot::new(
                                peer_id,
                                address,
                                Some(peer.handle())
                            )
                        );
                    }

                    let peer_map = peer_map.clone();

                    // Spawn the future as a concurrent task.
                    tokio::spawn(peer.then(move |_| {
                        {
                            let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");

                            peer_map.remove(&peer_id);
                        }

                        println!("node {} | node is killed.", node_id);

                        ::futures::future::ok(())
                    }))

                })

            });

        self.tcp_server = Some(Box::new(server));
    }

    pub fn maintain_peers(&mut self)
    {
        let peers = self.peers.write().expect("could not get peers lock");

        for (_, peer) in peers.iter() {

            if peer.has_peer() {
                println!("node already has peer.");
                continue;
            }

            // we dont try to connect to us.
            if &self.config.id == &peer.get_id() {
                println!("node already is self.");
                continue;
            }

            let address = peer.get_address().to_string();

            let tcp = TcpStream::connect(&address.parse().expect("could not parse peer url."));

            let peer_map = self.peers.clone();

            let raft_node_handle = self.handle().clone();

            let config_id = self.config.id;

            tokio::spawn(
                tcp
                    .map_err(move |e| {
                        println!("node {} | error on sock {} {:?}.", config_id, address, e);
                        ()
                    })
                    .and_then(move |tcp_stream| {
                        let mut hello_request = HelloRequest::new();
                        hello_request.set_node_id(config_id);

                        let stream = PeerStream::new(config_id, tcp_stream)
                            .with_hello_message(ProtocolMessage::Hello(hello_request));

                        // tcp_stream
                        PeerInflight::new(stream)
                    })
                    .and_then(move |(peer_id, peer_stream)| {

                        let address = peer_stream.get_address().to_string();

                        let peer = Peer::new(
                            peer_id,
                            raft_node_handle.clone(),
                            peer_stream,
                        );

                        {
                            let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");

                            if peer_map.get(&peer_id).is_some() {
                                println!("node {} | peer is already registered", config_id);
                                return Either::B(::futures::future::err(()));
                            }

                            peer_map.insert(NodePeerSlot::new(
                                peer_id,
                                address,
                                Some(peer.handle())
                            )).expect("foobar");
                        }

                        Either::A(peer)
                    })
                    .and_then(|_| {
                        ::futures::future::ok(())
                    })
                    .map_err(move |_| {
                        println!("node {} | peer killed.", config_id);
                        ()
                    })
            );

        }
    }

    pub fn send_propose(&mut self)
    {
        println!("node {} | propose a request", self.config.id);
    }
}


impl Future for RaftNode {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        self.listen();

        println!("node {} | poll!", self.config.id);

        // the loop is required because we need to make sure that the interval returns not ready,
        // because we need to ensure that the interval returns Async::NotReady to that this future will be
        // woken up ...
        loop {
            match self.interval.poll() {
                Ok(Async::Ready(_)) => {
                    println!("node {} | node interval is ready ...", &self.config.id);
                    self.maintain_peers();
                }
                Ok(Async::NotReady) => {
                    println!("node {} | node interval is not ready ...", &self.config.id);
                    break;
                }
                Err(e) => {
                    println!("node {} | problem with peer timer, teardown.", &self.config.id);
                    return Err(());
                }
            };
        }

        match &mut self.tcp_server {
            Some(ref mut t) => match t.poll() {
                Ok(t) => {
                    println!("node {} | tcp server poll ok.", &self.config.id);
                }
                Err(e) => {
                    println!("node {} | error on polling tcp server, teardown.", &self.config.id);
                    return Err(());
                }
            },
            None => panic!("cant poll when the tcp server isnt started. you need to call listen() before")
        };

        Ok(Async::NotReady) // look at the timer loop, we loop until it's not ready.
    }
}

pub enum RaftNodeCommand {}

pub struct RaftNodeHandle
{
    id: u64,
    sender: Sender<RaftNodeCommand>,
}

impl RaftNodeHandle {
    pub fn clone(&self) -> RaftNodeHandle {
        RaftNodeHandle {
            id: self.id,
            sender: self.sender.clone(),
        }
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn send(&mut self, command: RaftNodeCommand) {
        self.sender.try_send(command).expect("peer cannot communicate to raftNode");
    }
}