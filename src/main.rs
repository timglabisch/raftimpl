extern crate raft;

use std::thread;
use std::time::Duration;

fn main() {
    println!("Hello, world!");


    let mut handles = vec![];

    for i in 0..4 {
        let node_id = i;
        handles.push(thread::spawn(move || {
            loop {
                println!("hello from node {}", i);
                thread::sleep(Duration::from_secs(1));
            }
        }));
    }

    for h in handles {
        h.join();
    }


}
