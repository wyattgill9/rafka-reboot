use rkyv::{Archive, Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub enum BrokerError {
    TopicNotFound,
    PartitionNotFound,
    NotLeader,
    InvalidSequence,
    ReplicationError,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub topic:     String,
    pub payload:   Vec<u8>,
    pub timestamp: u64,
    pub seq:       u64,    
}

pub enum ClientRequest {
    CreateTopic {
        topic:          String,
        num_partitions: u32,
        rep_factor:     Option<u32> // replication factor
    },
    Subscribe {
        topic: String,
    },
    Unsubscribe {
        topic: String
    },
    Publish {
        topic:     String,
        payload:   Vec<u8>,
        timestamp: u64
    },
}

pub enum ClientResponse {
    Ok,
    TopicCreated { topic: String },
    Messages { messages: Vec<Message> },
    Error(BrokerError)
}

pub fn global_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u64
}
