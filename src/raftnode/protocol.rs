use bytes::BytesMut;
use byteorder::BigEndian;
use bytes::BufMut;
use byteorder::ByteOrder;
use raft::eraftpb::Message;
use protobuf::Message as ProtoMessage;
use byteorder::WriteBytesExt;
use byteorder::{LittleEndian};

const SIZE_16: usize = 2;
const SIZE_32: usize = 4;

const HEADER_SIZE: usize = 3 * SIZE_16 + 3 * SIZE_32;

pub enum ProtocolMessage {
    Hello(u64),
    // tcp client sends a hello with it's id.
    HelloAck(u64, u64),
    // we get back an ack. with the original id and the responder id.
    Raft(Message),
}

impl ProtocolMessage {
    pub const TYPE_RAFT: u16 = 1;

    pub fn encode_type(&self) -> u16 {
        match self {
            &ProtocolMessage::Raft(_) => 1,
            &ProtocolMessage::Hello(_) => 2,
            &ProtocolMessage::HelloAck(_, _) => 3,
            _ => {
                panic!("not supported.");
            }
        }
    }

    pub fn encode_body(&self) -> Result<Vec<u8>, String> {
        match self {
            &ProtocolMessage::Raft(ref m) => {
                Ok(m.write_to_bytes().expect("could not get bytes for message"))
            },
            &ProtocolMessage::Hello(id) => {
                println!("encoded Hello Message {}", id);
                let mut buf = vec![];
                LittleEndian::write_u64(&mut buf, id as u64);
                Ok(buf)
            },
            &ProtocolMessage::HelloAck(id, other) => {
                println!("encoded hello ack from peer {} -> {}", id, other);
                let mut buf = vec![];
                LittleEndian::write_u64(&mut buf, id as u64);
                LittleEndian::write_u64(&mut buf, other as u64);
                Ok(buf)
            },
            _ => {
                panic!("not supported.");
            }
        }
    }
}

#[derive(Debug)]
pub struct RawProtocolMessage {
    pub id: u32,
    pub header: BytesMut,
    pub body: BytesMut,
}

pub struct Protocol;

impl Protocol {
    pub fn decode(src: &mut BytesMut) -> Result<Option<ProtocolMessage>, String> {
        let len = src.len();

        if len < HEADER_SIZE {
            println!("waiting for bytes, already send {} bytes", len);
            return Ok(None);
        }

        //println!("protocol {:?}", BigEndian::read_u16(&src[0..4]));
        if BigEndian::read_u16(&src[0..2]) != 1337 {
            return Err("invalid protocol".into());
        }

        if BigEndian::read_u16(&src[2..4]) != 1 {
            return Err("invalid version".into());
        }

        let message_type = BigEndian::read_u16(&src[4..6]);

        let id = BigEndian::read_u32(&src[6..10]);

        let size_header = BigEndian::read_u32(&src[10..14]) as usize;

        let size_body = BigEndian::read_u32(&src[14..18]) as usize;

        if len < HEADER_SIZE + size_header + size_body {
            println!("missing bytes for content");
            return Ok(None);
        }

        let _ = src.split_to(HEADER_SIZE);

        let raw_protocol_message = RawProtocolMessage {
            id: id,
            header: src.split_to(size_header),
            body: src.split_to(size_body),
        };

        println!("got message");

        match message_type {
            ProtocolMessage::TYPE_RAFT => {
                match ::protobuf::parse_from_bytes::<Message>(&raw_protocol_message.body) {
                    Ok(m) => Ok(Some(ProtocolMessage::Raft(m))),
                    Err(_) => Err("could not parse message".to_string())
                }
            }
            _ => Err("unknown message".to_string())
        }
    }


    pub fn encode(protocol_message: ProtocolMessage, dst_bytes: &mut BytesMut) -> Result<(), String> {
        let body = protocol_message.encode_body()?;

        let raw_protocol_message = RawProtocolMessage {
            id: 1,
            header: BytesMut::new(),
            body: body.into(),
        };

        dst_bytes.reserve(HEADER_SIZE + raw_protocol_message.header.len() + raw_protocol_message.body.len());

        dst_bytes.put_u16::<BigEndian>(1337);
        dst_bytes.put_u16::<BigEndian>(1);
        dst_bytes.put_u16::<BigEndian>(protocol_message.encode_type());
        dst_bytes.put_u32::<BigEndian>(1);
        dst_bytes.put_u32::<BigEndian>(raw_protocol_message.header.len() as u32);
        dst_bytes.put_u32::<BigEndian>(raw_protocol_message.body.len() as u32);

        dst_bytes.put_slice(raw_protocol_message.header.as_ref());
        dst_bytes.put_slice(raw_protocol_message.body.as_ref());

        Ok(())
    }
}