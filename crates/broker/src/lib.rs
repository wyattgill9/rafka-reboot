#![allow(unused_imports, dead_code)]

use std::{
    collections::VecDeque,
    sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}}
};
use tokio::sync::mpsc; 
use dashmap::DashMap;

use util::global_time;

pub type RafkaResult<T> = std::result::Result<T, RafkaError>;

#[derive(Debug)]
pub enum RafkaError {
    
}


#[derive(Debug, Clone)]
pub struct Message {
    pub id:        u64,
    pub topic:     String, // addr
    pub payload:   Vec<u8>,
    pub timestamp: u64,
    pub partition: PartitionId,
}

pub type PartitionId = usize;

#[derive(Debug, Clone)]
pub struct Partition {
    pub id:        PartitionId,
    pub messages:  VecDeque<Message>,
    pub offset:    u64,
}

impl Partition {
    pub fn new(id: PartitionId) -> Self {
        Self {
            id,
            messages: VecDeque::new(),
            offset:   0
        }
    }

    pub fn append(&mut self, message: Message) {
        self.messages.push_back(message);
        self.offset += 1;
    }

    pub fn read_from(&self, offset: u64) -> impl Iterator<Item = &Message> {
        self.messages
            .iter()
            .skip(offset as usize) // only include after messages after the given "offset"
    }
}

#[derive(Debug)]
pub struct Topic {
    pub name:       String, // ie "8080"
    pub partitions: Vec<RwLock<Partition>>,

    // Metadata
    cur_partition:  AtomicUsize,
    pub cur_id:     AtomicUsize
}

impl Topic {
    pub fn new(name: String, partition_count: usize) -> Self {
        let mut partitions: Vec<RwLock<Partition>> = Vec::with_capacity(partition_count as usize);
        for p in 0..=partition_count {
            partitions.push(RwLock::new(Partition::new(p)));
        }

        Self {
            name,
            partitions,
            cur_partition: AtomicUsize::new(0),
            cur_id:        AtomicUsize::new(0)
        }
    }

    pub async fn publish(&self, payload: Vec<u8>) -> RafkaResult<()> {
        let part_idx = self.get_partition();

        let msg = Message {
            id: Self::get_new_message_id(&self),
            topic: self.name.clone() ,
            payload,
            timestamp: global_time(),
            partition: part_idx,
        };

        // Write to partition
        {
            let mut part = self.partitions[part_idx].write().unwrap();
            part.append(msg.clone())
        }

        // maybe notify subscribers idk

        Ok(())
    }

    /// Simple Round Robin
    fn get_partition(&self) -> PartitionId {
        let i = self.cur_partition.fetch_add(1, Ordering::Relaxed);
        i % self.partitions.len()
    }

    fn get_new_message_id(&self) -> u64 {
        let val = self.cur_id.fetch_add(1, Ordering::Relaxed);
        val.try_into().unwrap()
    }
}

#[derive(Debug)]
pub struct Broker {
    topics: DashMap<String, Topic>
}

impl Broker {
    pub fn new() -> Self {
        Self {
            topics: DashMap::new()
        }
    }

    pub async fn start(&self, _addr: &str) -> RafkaResult<()> {
        Ok(())
    }

    async fn handle_client(&self) -> RafkaResult<()> {
        Ok(())
    }

    async fn handle_command(&self) -> RafkaResult<()> {
        Ok(())
    }
}
