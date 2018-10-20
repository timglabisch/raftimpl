pub struct Peer {
    id : u64,
    connection : ()
}

impl Peer {
    pub fn new(id : u64, connection : ()) -> Self {
        Peer {
            id,
            connection
        }
    }
}