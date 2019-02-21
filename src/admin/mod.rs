use futures::Future;
use hyper::Server;
use hyper::service::service_fn_ok;
use hyper::Request;
use hyper::Body;
use hyper::Response;
use raftnode::node::RaftNodeHandle;
use tera::Tera;
use tera::Context;
use raftnode::peer::PeerIdent;
use serde::Serialize;

lazy_static! {
    pub static ref TERA: Tera = {
        let mut tera = compile_templates!("src/templates/**/*");

        tera
    };
}

pub struct Admin {
    node_id: u64,
    raftnode_handle: RaftNodeHandle
}

#[derive(Serialize)]
pub struct TemplatePeerInfo {
    peer_ident: PeerIdent
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

                    let template_peer_infos = {
                        let mut buf = vec![];
                        let read_lock = raftnode_handle.peers().clone();
                        let peers = read_lock.read().expect("could not get read lock");

                        for (peer_ident, _) in peers.iter() {
                            buf.push(TemplatePeerInfo {
                                peer_ident: peer_ident.clone()
                            });
                        }

                        buf
                    };


                    let mut context = Context::new();
                    context.insert("template_peer_infos", &template_peer_infos);
                    let rendered = TERA.render("foo.html.twig", &context);

                    let body = match rendered {
                        Ok(b) => b,
                        Err(e) => format!("{:?}", e)
                    };

                    Response::builder()
                        .header("content-type", "text/html")
                        .body(Body::from(body))
                        .unwrap()
                })
            })
            .map_err(|e| eprintln!("server error: {}", e))
            .and_then(|_| Ok(()))
    }
}

