// TODO move to u64 as usize is inconsitent on ALL systems

#![allow(unused_imports, dead_code)]

use std::{
    collections::VecDeque,
    sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}}
};

use tokio::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::{mpsc, Mutex},
    io::{AsyncWriteExt, AsyncReadExt}
};

// use rkyv::ser::{Serializer, serializers::AllocSerializer};
use rkyv::to_bytes;

use dashmap::DashMap;

use util::global_time;

pub type RafkaResult<T> = std::result::Result<T, RafkaError>;

#[derive(Debug, thiserror::Error)]
pub enum RafkaError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]   
    Serialization(String),
}

pub type PartitionId = usize;

#[derive(Debug, Clone)]
pub struct Message {
    pub id:        u64,
    pub topic:     String, // addr
    pub payload:   Vec<u8>,
    pub timestamp: u64,
    pub partition: PartitionId,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub enum RafkaCommand {
    Publish     { topic: String, payload: Vec<u8> },
    Subscribe   { topic: String },
    Unsubscribe { topic: String },
    CreateTopic { topic: String, partitions: u32 },
    ListTopics, // TODO
    Heartbeat,
}

pub enum RafkaResponse {
    Ok,
    Error(String),
    Message(Message),
    Topics(Vec<String>),
    Subscribed { topic: String },
}

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
    pub name:           String, // ie "8080"
    pub partitions:     Vec<Arc<RwLock<Partition>>>,

    // Metadata
    pub cur_partition:  AtomicUsize,
    pub cur_id:         AtomicUsize
}

impl Clone for Topic {
    fn clone(&self) -> Self {
        Self {
            name:          self.name.clone(),
            partitions:    self.partitions.clone(),
            cur_partition: AtomicUsize::new(self.cur_partition.load(Ordering::SeqCst)), 
            cur_id:        AtomicUsize::new(self.cur_id.load(Ordering::SeqCst)),
        }
    }
}

impl Topic {
    pub fn new(name: String, partition_count: usize) -> Self {
        let mut partitions: Vec<Arc<RwLock<Partition>>> = Vec::with_capacity(partition_count as usize);
        for p in 0..=partition_count {
            partitions.push(Arc::new(RwLock::new(Partition::new(p))));
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
    topics:         DashMap<String, Topic>,
    next_client_id: AtomicUsize
}

impl Clone for Broker {
    fn clone(&self) -> Self {
        Self {
            topics:         self.topics.clone(),
            next_client_id: AtomicUsize::new(self.next_client_id.load(Ordering::SeqCst))
        }
    }
}

impl Broker {
    pub fn new() -> Self {
        Self {
            topics:         DashMap::new(),
            next_client_id: AtomicUsize::new(0),
        }
    }

    pub async fn start(&self, addr: &str) -> RafkaResult<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("Broker on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("New client connected {}", addr);

            let broker = self.clone();
            let client_id = self.next_client_id.fetch_add(1, Ordering::SeqCst);

            tokio::spawn(async move {
                if let Err(e) = broker.handle_client(socket, client_id).await {
                    eprintln!("client_id {} err: {}", client_id, e);
                }
            });
        }       
    }

    // harder part
    async fn handle_client(&self, mut socket: TcpStream, client_id: usize) -> RafkaResult<()> {
        let (mut read_half, mut write_half) = socket.into_split();            // split the socket between an async reader and writer (they are concurrent)

        let write_half = Arc::new(Mutex::new(write_half));
        let (tx, mut rx) = mpsc::unbounded_channel::<RafkaResponse>();   // channel between our reader and the writer tasks

        let writer_clone = Arc::clone(&write_half);
        let write_task = tokio::spawn(async move {                       // spawn an async task that waits for the reader channel to send
            while let Some(resp) = rx.recv().await {                     // a message to our writer channel and if it gets one send the response back through the TcpStream 
                let mut writer = writer_clone.lock().await;
                if let Err(e) = Self::send_response(&mut *writer, &resp).await {
                    eprintln!("Failed to send RafkaResponse: {}", e);
                    break;
                }
            }
        });

        let mut buffer = vec![0u8; 8192];                                 // reciever task that waits to get a client response, puts it in a buffer and handles the given command
        loop {
            let n = read_half.read(&mut buffer).await?;

            if n == 0 {
                break; // the connection was closed
            }

            match self.handle_command(&buffer[..n], &tx).await {
                Ok(()) => {},
                Err(e) => {
                    let _ = tx.send(RafkaResponse::Error(e.to_string()));
                }
            }

            
        }
        
        write_task.abort();
        println!("Client {} disconnected", client_id);
        Ok(())
    }

    async fn handle_command(&self, data: &[u8], tx: &mpsc::UnboundedSender<RafkaResponse>) -> RafkaResult<()> {
        Ok(())
    }

    async fn send_response(
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        response: &RafkaResponse,
    ) -> RafkaResult<()> {
        let bytes = to_bytes::<_, 1024>(response)
            .map_err(|e| RafkaError::Serialization(e.to_string()))?;
    
        writer.write_u32(bytes.len() as u32).await?;
        writer.write_all(&bytes).await?;
        writer.flush().await?;

        Ok(())
    }



}
