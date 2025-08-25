#![allow(dead_code, unused_imports)]

use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
};

use dashmap::DashMap;

use tokio::{
    fs::symlink_metadata, io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::mpsc
};

use util::{global_time, ClientRequest, ClientResponse, Message, BrokerError};

type BrokerId    = u32;
type PartitionId = u32;
type TopicName   = String;

// A Partition 
pub struct PartitionReplica { 
    // topic: TopicName,

    partition_id: PartitionId,

    // InMemoryLog
    messages: Vec<Message>,
    seq_start: usize,

    // if leader
    is_leader: bool,
    followers: Option<Vec<BrokerId>>, 
}

impl PartitionReplica {
    pub fn new(partition_id: PartitionId, seq_start: usize, is_leader: bool) -> Self {
        Self {
            partition_id,
            messages: Vec::new(),
            seq_start,
            is_leader,
            followers: if is_leader { Some(Vec::new()) } else { None },
        }
    }

    pub fn append_message(&mut self, message: Message) -> Result<(), BrokerError> {
        if !self.is_leader {
            return Err(BrokerError::NotLeader);
        }

        self.messages.push(message);
        Ok(())
    }
    
    // maybe just return a &[Message] - much more efficient
    pub fn consume_from(&self, sequence: usize) -> Vec<Message> {
        if sequence > self.seq_start {
            return Vec::new();
        }

        let start_idx = sequence - self.seq_start;

        if start_idx >= self.messages.len() {
            Vec::new()
        } else {
            self.messages[start_idx..].to_vec()
        }
    }
   
    pub fn latest_sequence(&self) -> usize {
        self.seq_start + self.messages.len()
    }
    
    pub fn add_follower(&mut self, broker_id: BrokerId) -> Result<(), BrokerError> {
        if !self.is_leader {
            return Err(BrokerError::NotLeader);
        }

        if let Some(followers) = &mut self.followers {
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

pub struct Topic {
    name: String,

    partitions: DashMap<PartitionId, PartitionReplica>,
    
    // Metadata
    rep_factor: u32,
    created_at: u64,
    next_partition: AtomicUsize, // next partition to send message to
}

impl Topic {
    pub fn new(name: String, num_partitions: u32, rep_factor: u32) -> Self {
        let partitions = (0..num_partitions)
            .map(|i| {
                let is_leader = i == 0; // if its 0, leader is true
                let replica = PartitionReplica::new(i, 0, is_leader);
                (i, replica)
            })
            .collect();

        Self {
            name,
            partitions,
            rep_factor,
            created_at: global_time(),
            next_partition: AtomicUsize::new(0)
        }
    }

    pub fn get_partition_for_message(&self) -> PartitionId {
        let num_partitions = self.partitions.len();
        if num_partitions == 0 { return 0 }

        let next = self.next_partition.fetch_add(1, Ordering::SeqCst);
        (next % num_partitions) as u32 // technically i should move around the types cause usize % u32, but im lazy
    }

    pub fn publish_message(&self, message: Message) -> Result<(), BrokerError> {
        let part_id = self.get_partition_for_message();

        if let Some(mut partition) = self.partitions.get_mut(&part_id) {
            partition.append_message(message)
        } else {
            Err(BrokerError::PartitionNotFound)
        }
    }
}

pub struct Broker {
    id: BrokerId,
    addr: String, 
    topics: DashMap<TopicName, Topic>,

    is_controller: bool,
}

impl Broker {
    pub fn new(id: BrokerId, address: String) -> Self {
        Self {
            id,
            addr: address,
            topics: DashMap::new(),
            is_controller: id == 0
        }
    }

    pub async fn start(&self) {}

    async fn handle_client() {}

    async fn process_request(&mut self, request: ClientRequest) -> ClientResponse {
        match request {
            ClientRequest::CreateTopic { topic, num_partitions, rep_factor } => {
                let replication_factor = rep_factor.unwrap_or(1);
                let new_topic = Topic::new(topic.clone(), num_partitions, replication_factor);
                self.topics.insert(topic.clone(), new_topic);
                ClientResponse::TopicCreated { topic }
            },
            ClientRequest::Subscribe { topic } => {
                todo!();
            },
            ClientRequest::Unsubscribe { topic } => {
                todo!();
            },
            ClientRequest::Publish { topic, payload, timestamp } => {
                todo!();               
            },
        }
    }

    pub fn is_controller(&self) -> bool {
        self.is_controller
    }

    pub fn get_broker_id(&self) -> BrokerId {
        self.id
    }   
}
