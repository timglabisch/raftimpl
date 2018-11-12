use raftnode::peer::Peer;
use std::collections::HashMap;
use std::collections::hash_map::Iter;
use raftnode::peer::PeerHandle;

pub struct NodePeerSlot {
    peer: Option<PeerHandle>,
    address: String,
    peer_id: u64
}

impl NodePeerSlot {
    pub fn new(peer_id: u64, address : String, handle : Option<PeerHandle>) -> Self {
        NodePeerSlot {
            peer: handle,
            address,
            peer_id
        }
    }

    pub fn get_id(&self) -> u64 {
        self.peer_id
    }

    pub fn get_address(&self) -> &str { &self.address }

    pub fn has_peer(&self) -> bool {
        self.peer.is_some()
    }
}

pub struct PeerSlotMap {
    slots_identifier_map: HashMap<u64, NodePeerSlot>
}

impl PeerSlotMap {

    pub fn new() -> Self {
        PeerSlotMap {
            slots_identifier_map: HashMap::new()
        }
    }

    pub fn iter(&self) -> Iter<u64, NodePeerSlot> {
        self.slots_identifier_map.iter()
    }

    pub fn get(&self, index : &u64) -> Option<&NodePeerSlot>
    {
        self.slots_identifier_map.get(index)
    }

    pub fn remove(&mut self, index : &u64) -> Option<NodePeerSlot>
    {
        self.slots_identifier_map.remove(index)
    }

    pub fn insert(&mut self, node : NodePeerSlot) -> Result<(), ()> {

        if self.slots_identifier_map.get(&node.get_id()).is_some() {
            return Err(());
        }

        self.slots_identifier_map.insert(node.get_id(), node);

        Ok(())
    }
}