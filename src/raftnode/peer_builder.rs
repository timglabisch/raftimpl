use tokio::net::TcpStream;
use tokio::prelude::Future;
use tokio::prelude::Async;
use tokio::io::ReadHalf;
use tokio::io::WriteHalf;
use tokio::io::AsyncRead;

pub struct PeerBuilder {
    connection_read : ReadHalf<TcpStream>,
    connection_write : WriteHalf<TcpStream>,
}

impl PeerBuilder {

    pub fn from_tcp_stream(tcp_stream : TcpStream) -> PeerBuilder {

        let (connection_read, connection_write) = tcp_stream.split();

        PeerBuilder {
            connection_read,
            connection_write
        }
    }

}

/*
impl Future for PeerBuilder {

    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        self.tcp_stream.poll_read()
    }

}
*/