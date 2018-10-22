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

pub struct RaftNode {
    peer_counter: Arc<AtomicUsize>,
    // every new connection (aka. peer) will get a new id.
    config: Config,
    raw_node: RawNode<MemStorage>,
    tcp_server: Option<Box<Future<Item=(), Error=()> + Send>>,
    interval: Interval,
    channel_in_receiver: Receiver<RaftNodeCommand>,
    channel_in_sender: Sender<RaftNodeCommand>,
    peers: Arc<RwLock<HashMap<u64, RaftNodePeerInfo>>>,
}

pub struct RaftNodePeerInfo {
    id: u64,
    handle: PeerHandle,
}

impl RaftNode {

    pub fn handle(&self) -> RaftNodeHandle {
        RaftNodeHandle {
            sender: self.channel_in_sender.clone()
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


        RaftNode {
            peer_counter: Arc::new(AtomicUsize::new(0)),
            config,
            raw_node,
            tcp_server: None,
            interval: Interval::new_interval(Duration::from_millis(1000)),
            channel_in_receiver,
            channel_in_sender,
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn listen(&mut self) -> () {

        if self.tcp_server.is_some() {
            return ();
        }

        let port = format!("127.0.0.1:200{}", self.config.id);

        println!("runing node {} on port {}", self.config.id, port);

        let addr = port.parse().unwrap();

        let listener = TcpListener::bind(&addr)
            .expect("unable to bind TCP listener");

        let raft_node_handle = self.handle();


        let peer_counter = self.peer_counter.clone();
        let peer_map = self.peers.clone();

        let server = listener.incoming()
            .map_err(|e| eprintln!("accept failed = {:?}", e))
            .for_each(move |sock| {

                // die id ist hier nicht korrekt, die id muss vom client übermittelt werden.
                let peer_id = peer_counter.deref().fetch_add(1, Ordering::SeqCst) as u64;

                let peer = Peer::new(
                    peer_id,
                    raft_node_handle.clone(),
                    PeerStream::new(sock),
                );

                {
                    let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");

                    peer_map.insert(
                        peer_id,
                        RaftNodePeerInfo {
                            id: peer_id,
                            handle: peer.handle()
                        }
                    );
                }

                let peer_map = peer_map.clone();

                // Spawn the future as a concurrent task.
                tokio::spawn(peer.then(move |_| {

                    {
                        let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");

                        peer_map.remove(&peer_id);
                    }

                    println!("node is killed.");

                    Ok(())
                }))
            });

        self.tcp_server = Some(Box::new(server));
    }

    pub fn maintain_peers(&mut self)
    {
        let possible_nodes = vec![
            1,
            2,
            3,
        ];

        let peers = self.peers.write().expect("could not get peers lock");

        for peer_id in possible_nodes.iter() {

            // we dont try to connect to us.
            if &self.config.id == peer_id {
                continue;
            }

            if peers.get(peer_id).is_some() {
                println!("peer {} already exists, no need to try to contact it.", peer_id);
                continue;
            }

            let tcp = TcpStream::connect(&format!("127.0.0.1:200{}", peer_id).parse().expect("could not parse peer url."));

            let peer_map = self.peers.clone();

            let raft_node_handle = self.handle().clone();

            tokio::spawn(
            tcp
                .map_err(|_|{
                    println!("error on sock.");
                    ()
                })
                .and_then(|tcp_stream|{
                    PeerInflight::new(PeerStream::new(tcp_stream))
                })
                .and_then(move |(peer_id, peer_stream)|{


                    let peer = Peer::new(
                        peer_id,
                        raft_node_handle.clone(),
                        peer_stream
                    );

                   {
                       let mut peer_map = peer_map.deref().write().expect("could not get peer write lock");

                       if peer_map.get(&peer_id).is_some() {
                           println!("peer is already registered");
                           panic!("how to return this?");
                       }

                       peer_map.insert(peer_id, RaftNodePeerInfo {
                           id: peer_id,
                           handle: peer.handle()
                       });
                   }

                    peer
                })
                .and_then(|_| {
                    ::futures::future::ok(())
                })
            );
        }

    }

    pub fn send_propose(&mut self)
    {
        println!("propose a request");
    }
}


impl Future for RaftNode {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {

        self.listen();

        println!("poll!");

        // the loop is required because we need to make sure that the interval returns not ready,
        // because we need to ensure that the interval returns Async::NotReady to that this future will be
        // woken up ...
        loop {
            match self.interval.poll() {
                Ok(Async::Ready(_)) => {
                    println!("node interval {} is ready ...", &self.config.id);
                    self.maintain_peers();
                },
                Ok(Async::NotReady) => {
                    println!("node interval {} is not ready ...", &self.config.id);
                    break;
                },
                Err(e) => {
                    println!("problem with peer timer, teardown.");
                    return Err(())
                },
            };
        }

        match &mut self.tcp_server {
            Some(ref mut t) => match t.poll() {
                Ok(t) => {
                    println!("tcp server {} poll ok.", &self.config.id);
                },
                Err(e) => {
                    println!("error on polling tcp server {}, teardown.", &self.config.id);
                    return Err(())
                },
            },
            None => panic!("cant poll when the tcp server isnt started. you need to call listen() before")
        };

        Ok(Async::NotReady) // look at the timer loop, we loop until it's not ready.
    }
}

pub enum RaftNodeCommand {

}

pub struct RaftNodeHandle
{
    sender: Sender<RaftNodeCommand>,
}

impl RaftNodeHandle {
    pub fn clone(&self) -> RaftNodeHandle {
        RaftNodeHandle {
            sender: self.sender.clone()
        }
    }

    pub fn send(&mut self, command: RaftNodeCommand) {
        self.sender.try_send(command).expect("peer cannot communicate to raftNode");
    }
}