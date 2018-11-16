use tokio::net::TcpStream;
use tokio::prelude::Future;
use tokio::prelude::Async;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::io::AsyncRead;
use raftnode::peer_stream::PeerStream;
use raftnode::peer::Peer;
use raftnode::protocol::ProtocolMessage;
use tokio::io::ErrorKind::WouldBlock;
use protos::hello::{HelloRequest, HelloResponse};


pub struct PeerInflightActive {
    // we use a Option because we may move the stream out of this struct.
    stream : Option<PeerStream>
}

// es muss ein unterschied zwischen einem aktiven und einem passiven peerInflight geben.
impl PeerInflightActive {

    pub fn new(stream : PeerStream) -> PeerInflightActive {
        PeerInflightActive {
            stream: Some(stream)
        }
    }

    pub fn get_node_id(&self) -> u64 {
        match self.stream {
            Some(ref o) => o.get_node_id(),
            _ => unreachable!("peer inflight has no stream")
        }
    }

}

impl Future for PeerInflightActive {

    type Item = (u64, PeerStream);
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        loop {
            match self.stream.poll() {
                Ok(Async::NotReady) => {
                    println!("node {} | read all from Peer 2", self.get_node_id());
                    return Ok(Async::NotReady);
                },
                Ok(Async::Ready(msg)) => {

                    match msg {
                        Some(ProtocolMessage::HelloAck(m)) => {
                            match self.stream.take() {
                                Some(stream) => {
                                    return Ok(Async::Ready((m.get_request_node_id(), stream)));
                                },
                                None => {
                                    println!("node {}, got multiple acks, this is a protocol violation.", self.get_node_id());
                                    return Err(());
                                }
                            }
                        },
                        None => {
                            continue;
                        }
                        Some(m) => {
                            println!("node {} | got unsupported message {:?} while contacting node.", self.get_node_id(), m);
                            return Err(());
                        }
                    };

                    continue;
                },
                Err(e) => {
                    println!("node {} | peer has an error, teardown.", self.get_node_id());
                    return Err(())
                },
            };
        };
    }

}



pub struct PeerInflightPassiv {
    // we use a Option because we may move the stream out of this struct.
    stream : Option<PeerStream>
}

// es muss ein unterschied zwischen einem aktiven und einem passiven peerInflight geben.
impl PeerInflightPassiv {

    pub fn new(stream : PeerStream) -> PeerInflightPassiv {
        PeerInflightPassiv {
            stream: Some(stream)
        }
    }

    pub fn get_node_id(&self) -> u64 {
        match self.stream {
            Some(ref o) => o.get_node_id(),
            _ => unreachable!("peer inflight passiv has no stream")
        }
    }

}

impl Future for PeerInflightPassiv {

    type Item = (u64, PeerStream);
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        loop {
            match self.stream.poll() {
                Ok(Async::NotReady) => {
                    println!("node {} | read all from Peer 2", self.get_node_id());
                    return Ok(Async::NotReady);
                },
                Ok(Async::Ready(msg)) => {

                    match msg {
                        Some(ProtocolMessage::Hello(m)) => {
                            let node_id = self.get_node_id();
                            match self.stream.take() {
                                Some(stream) => {
                                    println!("got hello, not we need to send a hello Ack");

                                    let mut hello_response = HelloResponse::new();
                                    hello_response.set_response_node_id(node_id);
                                    hello_response.set_request_node_id(m.get_node_id());

                                    let stream = stream.with_hello_message(ProtocolMessage::HelloAck(hello_response));
                                    return Ok(Async::Ready((m.get_node_id(), stream)));
                                },
                                None => {
                                    println!("node {}, got multiple hello's, this is a protocol violation.", self.get_node_id());
                                    return Err(());
                                }
                            }
                        },
                        None => {
                            continue;
                        }
                        Some(m) => {
                            println!("node {} | got unsupported message {:?} while contacting node.", self.get_node_id(), m);
                            return Err(());
                        }
                    };

                    continue;
                },
                Err(e) => {
                    println!("node {} | peer has an error, teardown.", self.get_node_id());
                    return Err(())
                },
            };
        };
    }

}