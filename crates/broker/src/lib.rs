#![allow(dead_code, unused_imports)]

use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::atomic::AtomicUsize
};

use common::Message;

pub enum BrokerError {
    TopicNotFound,
    PartitionNotFound,
    NotLeader,
    InvalidSequence,
    ReplicationError,
}

type BrokerId    = u32;
type PartitionId = u32;
type TopicName   = String;

// Simple in-memory log system, TODO: move to disk storage like kafka
struct InMemLog {
    messages: VecDeque<Message>,
    seq_start: u64,
}

impl InMemLog {
    fn new(seq_start: u64) -> Self {
        Self {
            messages: VecDeque::new(),
            seq_start
        }
    }

    fn append(&mut self, message: Message) {
        self.messages.push_back(message);
    }

    fn consume_from(&self, sequence: u64) -> Option<impl Iterator<Item=&Message>> {
        let index = (sequence.saturating_sub(self.seq_start)) as usize;
        let (a, b) = self.messages.as_slices();

        if index >= a.len() + b.len() {
            return None;
        }

        let iter = a.iter().chain(b.iter()).skip(index);
        Some(iter)
    }
    
    fn latest_sequence(&self) -> u64 {
        self.seq_start + self.messages.len() as u64
    }
}

// A Partition 
pub struct PartitionCopy { 
    topic: TopicName,
    partition_id: PartitionId,
    log: InMemLog,

    // if leader
    is_leader: bool,
    followers: Option<Vec<BrokerId>>, 
}

impl PartitionCopy {
    pub fn new(topic: TopicName, partition_id: PartitionId, seq_start: u64, is_leader: bool) -> Self {
        Self {
            topic,
            partition_id,
            log: InMemLog::new(seq_start),
            is_leader,
            followers: if is_leader { Some(Vec::new()) } else { None },
        }
    }
    
    pub fn append_message(&mut self, message: Message) -> Result<u64, BrokerError> {
        if !self.is_leader {
            return Err(BrokerError::NotLeader);
        }
        
        let sequence = self.log.latest_sequence();
        self.log.append(message);
        Ok(sequence)
    }
    
    pub fn consume_from(&self, sequence: u64) -> Option<impl Iterator<Item=&Message>> {
        self.log.consume_from(sequence)
    }
    
    pub fn latest_sequence(&self) -> u64 {
        self.log.latest_sequence()
    }
    
    pub fn add_follower(&mut self, broker_id: BrokerId) -> Result<(), BrokerError> {
        if !self.is_leader {
            return Err(BrokerError::NotLeader);
        }

        // take some mutable reference to our followers and push the new broker (if its not already there)
        if let Some(ref mut followers) = self.followers {
            if !followers.contains(&broker_id) {
                followers.push(broker_id);
            }
        }
        Ok(())
    }
    
    pub fn remove_follower(&mut self, broker_id: BrokerId) -> Result<(), BrokerError> {
        if !self.is_leader {
            return Err(BrokerError::NotLeader);
        }
        
        if let Some(followers) = &mut self.followers {
            if let Some(pos) = followers.iter().position(|&x| x == broker_id) {
                followers.remove(pos);
            }
        }
        Ok(())
    }
}

pub struct Broker {
    id: BrokerId,
    partitions: HashMap<(TopicName, PartitionId), PartitionCopy>,
}

impl Broker {
    pub fn new(id: BrokerId) -> Self {
        Self {
            id,
            partitions: HashMap::new(),
        }
    }

    pub fn create_partition(&mut self, topic: TopicName, partition_id: PartitionId, is_leader: bool) {
        let key = (topic.clone(), partition_id);
        let partition = PartitionCopy::new(topic, partition_id, 0, is_leader);
        self.partitions.insert(key, partition);
    }

    pub fn get_broker_id(&self) -> BrokerId {
        return self.id;    
    }   
}
