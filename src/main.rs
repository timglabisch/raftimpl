#![allow(dead_code)]
extern crate raft;

use std::thread;
use std::time::Duration;
use raftnode::node::RaftNode;

mod raftnode;

fn main() {
    println!("Hello, world!");


    let mut handles = vec![];

    for i in 0..4 {
        handles.push(thread::spawn(move || {

            let _ = RaftNode::new(i);

            loop {
                println!("hello from node {}", i);
                thread::sleep(Duration::from_secs(1));
            }
        }));
    }

    for h in handles {
        h.join().expect("join failed");
    }


}
