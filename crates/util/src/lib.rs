use std::time::{UNIX_EPOCH, SystemTime};

use rkyv::{
    Archive, Deserialize, Serialize
};

pub type RafkaResult<T> = std::result::Result<T, RafkaError>;

#[derive(Debug, thiserror::Error)]
pub enum RafkaError {
    #[error("Io error: {0}")]
    Io(String),

    #[error("Serialization error: {0}")]   
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),
}

impl From<std::io::Error> for RafkaError {
    fn from(error: std::io::Error) -> Self {
        RafkaError::Io(error.to_string())
    }
}

pub type PartitionId = u32;

#[derive(Archive, Serialize, Deserialize)]
#[derive(Debug, Clone)]
pub struct Message {
    pub id:        u64,
    pub topic:     String, // addr
    pub payload:   Vec<u8>,
    pub timestamp: u64,
    pub partition: PartitionId,
}

#[derive(Archive, Serialize, Deserialize)]
#[derive(Debug)]
pub enum RafkaCommand {
    Publish     { topic: String, payload: Vec<u8> },
    Subscribe   { topic: String },
    Unsubscribe { topic: String },
    CreateTopic { topic: String, partition_count: u32 },
    ListTopics,
}

#[derive(Archive, Serialize, Deserialize)]
#[derive(Debug)]
pub enum RafkaResponse  {
    Ok, // self
    Error(String), //self
    Message(Message), // publish
    Topics(Vec<String>), // list topics
    Subscribed { topic: String }, // self
}


pub fn global_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u64
}
