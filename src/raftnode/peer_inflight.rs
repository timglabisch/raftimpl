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

pub struct PeerInflight {
    // we use a Option because we may move the stream out of this struct.
    stream : Option<PeerStream>
}

impl PeerInflight {

    pub fn new(stream : PeerStream) -> PeerInflight {
        PeerInflight {
            stream: Some(stream)
        }
    }

}

impl Future for PeerInflight {

    type Item = (u64, PeerStream);
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        loop {
            match self.stream.poll() {
                Ok(Async::NotReady) => {
                    println!("read all from Peer 2");
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
                                    println!("got multiple acks, this is a protocol violation.");
                                    return Err(());
                                }
                            }
                        },
                        None => {
                            continue;
                        }
                        _ => {
                            println!("got unsupported message while contacting node.");
                            return Err(());
                        }
                    };

                    continue;
                },
                Err(e) => {
                    println!("peer has an error, teardown.");
                    return Err(())
                },
            };
        };
    }

}