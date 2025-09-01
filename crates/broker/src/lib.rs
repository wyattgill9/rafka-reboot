// TODO move to u64 as usize is inconsitent on ALL systems

#![allow(unused_imports, dead_code, unused_mut)]

use std::{
    collections::VecDeque,
    sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}}
};

use tokio::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::{mpsc, Mutex, broadcast},
    io::{AsyncWriteExt, AsyncReadExt}
};

use rkyv::{
    deserialize, rancor::Error, to_bytes, Archive, Archived, Deserialize, DeserializeUnsized, Serialize
};

use dashmap::DashMap;

use util::global_time;

pub type RafkaResult<T> = std::result::Result<T, RafkaError>;


#[derive(Debug, thiserror::Error)]
pub enum RafkaError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]   
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String)
}

pub type PartitionId = usize;

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
    CreateTopic { topic: String, partitions: u32 },
    ListTopics, // TODO
}

#[derive(Archive, Serialize, Deserialize)]
pub enum RafkaResponse {
    Ok, // self
    Error(String), //self
    Message(Message), // publish
    Topics(Vec<String>), // list topics
    Subscribed { topic: String }, // self
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

    // Clients
    pub subscribers:    broadcast::Sender<Message>,

    // Metadata
    pub cur_partition:  AtomicUsize,
    pub cur_id:         AtomicUsize
}

impl Clone for Topic {
    fn clone(&self) -> Self {
        Self {
            name:          self.name.clone(),
            partitions:    self.partitions.clone(),
            subscribers:   self.subscribers.clone(),
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

        let (tx, _) = broadcast::channel(100);

        Self {
            name,
            partitions,
            subscribers:   tx,
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
            part.append(msg.clone());
        } // RAII to release the lock instead of calling drop(part);

        // notify all the subscribers (clientele)
        let _ = self.subscribers.send(msg);
           
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

    pub async fn start(
        &self,
        addr: &str
    ) -> RafkaResult<()> {
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

    //         ┌───────────────┐
    // tx1 ───▶│               │
    // tx2 ───▶│    CHANNEL    │──▶ rx
    // tx3 ───▶│               │
    //         └───────────────┘

    async fn handle_client(
        &self,
        socket: TcpStream,
        client_id: usize
    ) -> RafkaResult<()> {
        let (mut reader, mut writer) = socket.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<RafkaResponse>();

        // writer task
        let writer_task = {
            tokio::spawn(async move {
                while let Some(resp) = rx.recv().await {    // wait to get a responce from the reader
                    if let Err(e) = Self::send_response(&mut writer, &resp).await { // send it through tcp to the client(s)
                        eprintln!("Failed to send client response: {}", e);
                        break;
                    }
                }
            })
        };

        let mut buffer = vec![0u8; 8192];

        while let Ok(n) = reader.read(&mut buffer).await {
            if n == 0 {
                break;
            }
            if let Err(e) = self.handle_command(&buffer[..n], &tx).await {
                let _ = tx.send(RafkaResponse::Error(e.to_string()));
            }
        }

        writer_task.abort();
        println!("Client {} disconnected", client_id);
        Ok(())
    }

    async fn handle_command(
        &self,
        data: &[u8],
        tx: &mpsc::UnboundedSender<RafkaResponse> // transmitter/sender
    ) -> RafkaResult<()> {
        if data.len() <= 4 {
            return Err(RafkaError::Deserialization("Command is to short".to_string()));
        }

        // get length of the messages - unsafe is 2 instructions faster smh
        //  let len = unsafe {
        //      let ptr = data.as_ptr() as *const u32;
        //      ptr.read_unaligned()
        //  };

        let len = usize::from_ne_bytes(data[..4].try_into().unwrap());

        // deserialize from start of payload to end pos
        let archived = rkyv::access::<Archived<RafkaCommand>, Error>(&data[4..4+len]).unwrap(); 
        let command  = deserialize::<RafkaCommand, Error>(archived).unwrap();

        println!("{:?}", command);
        
        match command {
            RafkaCommand::Publish {
                topic,
                payload
            } => {
                let topic = self.topics.get(&topic).unwrap(); // dashmap::Ref<T> derefs to &T
                topic.publish(payload).await?;
                tx.send(RafkaResponse::Ok).unwrap();
            },
            RafkaCommand::Subscribe { topic } => {
                
            },
            RafkaCommand::Unsubscribe { topic } => {
                
            },
            RafkaCommand::CreateTopic { topic, partitions } => {
                
            },
            RafkaCommand::ListTopics => {
                let topics: Vec<String> = self.topics
                                .iter()
                                .map(|entry| entry.key().clone())
                                .collect();

                let resp = RafkaResponse::Topics(topics);
                tx.send(resp).unwrap();
            },
        }
        Ok(())
    }

    // -- Outbound/Inbound Protocol Specs --
    //
    // [Payload-Length (u32)] - [Payload]

    async fn send_response(
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        response: &RafkaResponse,
    ) -> RafkaResult<()> {

        let bytes = rkyv::to_bytes::<Error>(response).unwrap();
        println!("{:?}", bytes);
          
        writer.write_u32(bytes.len() as u32).await?; 
        writer.write_all(&bytes).await?;
        writer.flush().await?;

        Ok(())
    }
}
