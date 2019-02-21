use futures::Future;
use hyper::Server;
use hyper::service::service_fn_ok;
use hyper::Request;
use hyper::Body;
use hyper::Response;
use raftnode::node::RaftNodeHandle;

pub struct Admin {
    node_id: u64,
    raftnode_handle: RaftNodeHandle
}

impl Admin {
    pub fn new(node_id: u64, raftnode_handle : RaftNodeHandle) -> Admin
    {
        Admin {
            node_id,
            raftnode_handle
        }
    }

    pub fn run_future(self) -> impl Future<Item=(), Error=()> {
        let addr = ([127, 0, 0, 1], 3000 + self.node_id as u16).into();

        let raftnode_handle = self.raftnode_handle.clone();

        Server::bind(&addr)
            .serve(move || {

                let raftnode_handle = raftnode_handle.clone();

                service_fn_ok(move |_: Request<Body>| {

                    raftnode_handle.get_id();

                    Response::new(Body::from("Hello World!"))
                })
            })
            .map_err(|e| eprintln!("server error: {}", e))
            .and_then(|_| Ok(()))
    }
}

