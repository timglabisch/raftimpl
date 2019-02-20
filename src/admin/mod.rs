use futures::Future;
use hyper::Server;
use hyper::service::service_fn_ok;
use hyper::Request;
use hyper::Body;
use hyper::Response;

pub struct Admin;

impl Admin {
    pub fn new() -> impl Future<Item=(), Error=()>
    {
        let addr = ([127, 0, 0, 1], 3000).into();

        Server::bind(&addr)
            .serve(|| {
                // This is the `Service` that will handle the connection.
                // `service_fn_ok` is a helper to convert a function that
                // returns a Response into a `Service`.
                service_fn_ok(move |_: Request<Body>| {
                    Response::new(Body::from("Hello World!"))
                })
            })
            .map_err(|e| eprintln!("server error: {}", e))
            .and_then(|_| Ok(()))
    }
}

