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
use raftnode::peer_inflight::PeerInflightActive;
use raftnode::protocol::ProtocolMessage;
use protos::hello::HelloRequest;
use futures::future::Either;
use raftnode::node_peer_slot::PeerSlotMap;
use raftnode::node_peer_slot::NodePeerSlot;
use raftnode::peer_inflight::PeerInflightPassiv;
use raftnode::peer::PeerCommand;
use protos::hello::PingRequest;
use std::sync::Mutex;
use raftnode::peer::PeerIdent;

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

    pub fn print_debug(&self) -> String
    {
        let mut buffer = String::new();

        let peers = self.peers.clone();

        for (id, slot) in peers.read().expect("could not get read lock").iter() {
            match slot.get_peer() {
                None => {
                    buffer.push_str(&format!("{:?} | Empty |\n", id))
                }
                Some(p) => {

                    let metrics = p.get_metrics().get_copy();

                    buffer.push_str(&format!(
                        "{:?} | {:?} | {:?} | {:?} | {:?} | {:?} |\n",
                        id,
                        p.get_state().get_state(),
                        metrics.get_created_at(),
                        metrics.get_connected_since(),
                        metrics.get_ping_requests(),
                        metrics.get_ping_responses(),
                    ))
                }
            }
        }

        buffer
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
        peer_slot_map.insert(NodePeerSlot::new(PeerIdent::new(1), "127.0.0.1:2001".into(), None));
        peer_slot_map.insert(NodePeerSlot::new(PeerIdent::new(2), "127.0.0.1:2002".into(), None));
        // peer_slot_map.insert(NodePeerSlot::new(3, "127.0.0.1:2003".into(), None));

        RaftNode {
            peer_counter: Arc::new(AtomicUsize::new(0)),
            config,
            raw_node,
            tcp_server: None,
            interval: Interval::new_interval(Duration::from_millis(5000)),
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

                let stream = PeerStream::new(node_id, tcp_stream);

                let raft_node_handle = raft_node_handle.clone();
                let peer_map = peer_map.clone();

                PeerInflightPassiv::new(stream)
                    .and_then(move |(peer_ident, peer_stream)|{

                    let address = peer_stream.get_address().to_string();

                    let peer = Peer::new(
                        peer_ident,
                        raft_node_handle.clone(),
                        peer_stream,
                    );

                    {
                        let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");

                        peer_map.insert(
                            NodePeerSlot::new(
                                peer_ident,
                                address,
                                Some(peer.handle())
                            )
                        );
                    }

                    let peer_map = peer_map.clone();

                    // Spawn the future as a concurrent task.
                    tokio::spawn(peer.then(move |peer_result| {
                        match peer_result {
                            /*Ok(peer) => {
                                let mut peer_map = peer_map2.deref().write().expect("could not get peer write lock");
                                peer_map.remove(&peer.get_id());

                                println!("peer {} finished. had {} pings and {} results.", &peer.get_id(), &peer.get_successful_ping_requests(),  &peer.get_successful_ping_responses());
                            },*/
                            Ok(peer_ident) => {
                                let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");
                                peer_map.remove(&peer_ident);

                                println!("peer {:?} finished unsuccessful.", &peer_ident);
                            }
                            Err(peer_ident) if !peer_ident.is_anon()  => {
                                let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");
                                peer_map.remove(&peer_ident);

                                println!("peer {:?} finished unsuccessful.", &peer_ident);
                                return return ::futures::future::err(());
                            }
                            Err(_) => {
                                return return ::futures::future::err(());
                            }
                        }

                        println!("node {:?} | node is killed.", peer_ident);

                        ::futures::future::ok(())
                    }))

                })

            });

        self.tcp_server = Some(Box::new(server));
    }

    pub fn rand_try_to_contact_to_peer(&self) -> bool
    {
        use ::rand::Rng;

        let mut rng = ::rand::thread_rng();
        let y: f64 = rng.gen();

        y < 0.10
    }

    pub fn maintain_peers(&mut self)
    {
        let peers = self.peers.write().expect("could not get peers lock");

        for (_, peer) in peers.iter() {

            match peer.get_peer() {
                Some(mut p) => {
                    println!("node already has peer.");

                    let mut ping_request = PingRequest::new();
                    ping_request.set_request_node_id(self.config.id);
                    ping_request.set_response_node_id(peer.get_ident().get_id());

                    match p.send(PeerCommand::OutgoingMessage(ProtocolMessage::PingRequest(ping_request))) {
                        Err(_) => { println!("node {} | could not send to peer {}", self.config.id, peer.get_ident().get_id()); },
                        _ => {}
                    }

                    continue;
                },
                None => {
                    // go on ..
                }
            }


            // we dont try to connect to us.
            if &self.config.id == &peer.get_ident().get_id() {
                //println!("node already is self.");
                continue;
            }

            if self.config.id == 1 {
                continue;
            }

            if (!self.rand_try_to_contact_to_peer()) {
                continue;
            }

            let address = peer.get_address().to_string();

            let tcp = TcpStream::connect(&address.parse().expect("could not parse peer url."));

            let peer_map = self.peers.clone();
            let peer_map2 = self.peers.clone();

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
                        PeerInflightActive::new(stream)
                    })
                    .map_err(|_| Err(PeerIdent::new_anon()) )
                    .and_then(move |(peer_ident, peer_stream)| {

                        let address = peer_stream.get_address().to_string();

                        let peer = Peer::new(
                            peer_ident,
                            raft_node_handle.clone(),
                            peer_stream,
                        );

                        {
                            let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");

                            match  peer_map.get(&peer_ident) {
                                Some(ref p) => if p.has_peer() {
                                    println!("node {} | peer is already registered", config_id);
                                    return Either::B(::futures::future::err(peer_ident.clone()));
                                },
                                _ => {}
                            };

                            match peer_map.insert(NodePeerSlot::new(
                                peer_ident,
                                address,
                                Some(peer.handle())
                            )) {
                                Ok(_) => {},
                                Err(e) => {
                                    println!("error: could not insert peer into map: {:?}", e);

                                    return Either::B(::futures::future::err(peer_ident.clone()));
                                }
                            };

                        }

                        Either::A(peer)
                    })
                    .then(move |peer_result| {

                        match peer_result {
                            Ok(peer_ident) => {
                                let mut peer_map = peer_map2.deref().write().expect("could not get peer write lock");
                                peer_map.remove(&peer_ident);

                                println!("peer {:?} finished unsuccessful.", &peer_ident);
                                ::futures::future::ok(peer_ident)

                            }
                            Err(peer_ident) if !peer_ident.is_anon()  => {
                                let mut peer_map = peer_map2.deref().write().expect("could not get peer write lock");
                                peer_map.remove(&peer_ident);

                                println!("peer {:?} finished unsuccessful.", &peer_ident);
                                ::futures::future::err(peer_ident)
                            }
                            Err(peer_ident) => {
                                ::futures::future::err(peer_ident)
                            }
                        }
                    })
                    .then(move |peer_result| {
                        println!("node {} | peer killed.", config_id);
                        Ok(())
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
                    print!("\n{}\n", &self.print_debug());
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