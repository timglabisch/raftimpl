#![allow(unused)]
#![allow(dead_code)]
#![allow(deprecated)]
extern crate raft;
extern crate tokio;
#[macro_use]
extern crate futures;
extern crate bytes;
extern crate byteorder;
extern crate protobuf;
extern crate rand;

use raftnode::node::RaftNode;
use futures::select_all;
use futures::prelude::*;

mod raftnode;
mod protos;

fn main() {

    let node_id = ::std::env::args().skip(1).next().expect("first argument required").parse::<u64>().expect("argument must be an u64");

    ::tokio::run(
        select_all(vec![
            RaftNode::new(node_id),
            // RaftNode::new(2),
        //   RaftNode::new(3),
        ]).and_then(|_|{
            Ok(())
        }).map_err(|_| {
            ()
        })
    );
}
