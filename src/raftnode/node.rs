use raft::storage::MemStorage;
use raft::Config;
use raft::RawNode;
use futures::Future;
use futures::Async;

pub struct RaftNode {
    config: Config,
    raw_node: RawNode<MemStorage>
}

impl Future for RaftNode {

    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        unimplemented!()
    }
}

impl RaftNode {
    pub fn new(node_id : u64) -> Self {

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


        RaftNode {
            config,
            raw_node
        }
    }

    pub fn send_propose(&mut self)
    {
        println!("propose a request");
    }
}