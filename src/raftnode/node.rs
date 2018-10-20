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

//use futures::try_ready;


pub struct RaftNode {
    peer_counter: u64,
    // every new connection (aka. peer) will get a new id.
    config: Config,
    raw_node: RawNode<MemStorage>,
    tcp_server: Option<Box<Future<Item=(), Error=()> + Send>>,
}

impl Future for RaftNode {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        match &mut self.tcp_server {
            Some(ref mut t) => t.poll(),
            None => panic!("cant poll when the tcp server isnt started. you need to call listen() before")
        }
    }
}

impl RaftNode {
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

        RaftNode {
            peer_counter: 0,
            config,
            raw_node,
            tcp_server: None,
        }
    }

    pub fn listen(&mut self) -> () {

        if self.tcp_server.is_some() {
            panic!("you cant call listen twice");
        }

        let port = format!("127.0.0.1:200{}", self.config.id);

        println!("runing node {} on port {}", self.config.id, port);

        let addr = port.parse().unwrap();

        let listener = TcpListener::bind(&addr)
            .expect("unable to bind TCP listener");

        let node_id = self.config.id;

        let server = listener.incoming()
            .map_err(|e| eprintln!("accept failed = {:?}", e))
            .for_each(move |sock| {

                // Spawn the future as a concurrent task.
                tokio::spawn(Peer::new(
                    node_id,
                    sock
                ))
            });

        self.tcp_server = Some(Box::new(server));
    }

    pub fn send_propose(&mut self)
    {
        println!("propose a request");
    }
}