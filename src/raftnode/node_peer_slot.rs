use raftnode::peer::Peer;
use std::collections::HashMap;
use std::collections::hash_map::Iter;
use raftnode::peer::PeerHandle;
use raftnode::peer::PeerIdent;

pub struct NodePeerSlot {
    peer: Option<PeerHandle>,
    address: String,
    peer_ident: PeerIdent
}

impl NodePeerSlot {
    pub fn new(peer_ident: PeerIdent, address : String, handle : Option<PeerHandle>) -> Self {
        NodePeerSlot {
            peer: handle,
            address,
            peer_ident
        }
    }

    pub fn get_ident(&self) -> &PeerIdent {
        &self.peer_ident
    }

    pub fn get_address(&self) -> &str { &self.address }

    pub fn has_peer(&self) -> bool {
        self.peer.is_some()
    }

    pub fn get_peer(&self) -> Option<PeerHandle> {
        match &self.peer {
            Some(ref p) => Some(p.clone()),
            None => None
        }
    }
}

pub struct PeerSlotMap {
    slots_identifier_map: HashMap<PeerIdent, NodePeerSlot>
}

impl PeerSlotMap {

    pub fn new() -> Self {
        PeerSlotMap {
            slots_identifier_map: HashMap::new()
        }
    }

    pub fn iter(&self) -> Iter<PeerIdent, NodePeerSlot> {
        self.slots_identifier_map.iter()
    }

    pub fn get(&self, peer_ident : &PeerIdent) -> Option<&NodePeerSlot>
    {
        self.slots_identifier_map.get(peer_ident)
    }

    pub fn remove(&mut self, peer_ident : &PeerIdent)
    {
        println!("remove peer {:?}", peer_ident);
        self.slots_identifier_map.remove(peer_ident).expect("could not drop index");
    }

    pub fn insert(&mut self, node : NodePeerSlot) -> Result<(), ()> {

        let peer_ident = node.get_ident().clone();

        println!("insert peer {:?}", &peer_ident);
        if self.slots_identifier_map.insert(peer_ident.clone(), node).is_some() {
            println!("replace peer {:?}", &peer_ident);
        }

        Ok(())
    }
}