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
extern crate hyper;

use raftnode::node::RaftNode;
use futures::select_all;
use futures::prelude::*;
use admin::Admin;

mod raftnode;
mod protos;
mod admin;

fn main() {

    let node_id = ::std::env::args().skip(1).next().expect("first argument required").parse::<u64>().expect("argument must be an u64");

    let raftnode = RaftNode::new(node_id);

    let futures : Vec<Box<Future<Item=(), Error=()> + Send>> = vec![
        Box::new(Admin::new(node_id, raftnode.handle()).run_future()),
        Box::new(raftnode),
    ];

    ::tokio::run(
        select_all(futures)
        .and_then(|_|{
            Ok(())
        }).map_err(|_| {
            ()
        })
    );
}
