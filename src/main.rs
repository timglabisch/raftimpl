#![allow(dead_code)]
extern crate raft;
extern crate tokio;
extern crate futures;

use raftnode::node::RaftNode;
use futures::select_all;
use futures::prelude::*;

mod raftnode;

fn main() {
    ::tokio::run(
        select_all(vec![
            RaftNode::new(1),
            RaftNode::new(2),
            RaftNode::new(3),
        ]).and_then(|_|{
            Ok(())
        }).map_err(|_| {
            ()
        })
    );
}
