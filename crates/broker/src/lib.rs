use std::{
    collections::VecDeque,
    sync::{
        atomic::{
            AtomicU32, AtomicU64, AtomicUsize, Ordering
        },
        Arc, RwLock
    }
};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, broadcast},
    io::{AsyncWriteExt, AsyncReadExt}
};

use rkyv::{
    access, deserialize, rancor::Error, Archived
};

use dashmap::DashMap;

use util::{
    PartitionId,
    Message,
    RafkaCommand, RafkaResponse,
    RafkaResult, RafkaError,
    global_time
};

const CHANNEL_BUFF_SIZE: usize = 1000;

#[derive(Debug)]
pub struct Partition {
    pub id:        PartitionId,
    pub messages:  VecDeque<Message>,
    pub offset:    AtomicU64,
}

impl Partition {
    pub fn new(id: PartitionId) -> Self {
        Self {
            id,
            messages: VecDeque::new(),
            offset:   AtomicU64::new(0),
        }
    }

    pub fn append(&mut self, message: Message) -> u64 {
        let offset = self.offset.fetch_add(1, Ordering::SeqCst);
        self.messages.push_back(message);
        offset
    }

    pub fn read_from(&self, offset: u64) -> impl Iterator<Item = &Message> {
        self.messages
            .iter()
            .skip(offset as usize)
    }

    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn current_offset(&self) -> u64 {
        self.offset.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct Topic {
    pub name:           String,
    pub partitions:     Vec<Arc<RwLock<Partition>>>,

    // Clients
    pub subscribers:    broadcast::Sender<Message>,

    // Metadata
    pub cur_partition:  AtomicU32,
    pub cur_id:         AtomicU32
}

impl Clone for Topic {
    fn clone(&self) -> Self {
        Self {
            name:          self.name.clone(),
            partitions:    self.partitions.clone(),
            subscribers:   self.subscribers.clone(),
            cur_partition: AtomicU32::new(self.cur_partition.load(Ordering::SeqCst)), 
            cur_id:        AtomicU32::new(self.cur_id.load(Ordering::SeqCst)),
        }
    }
}

impl Topic {
    pub fn new(name: String, partition_count: u32) -> Self {
        let partitions: Vec<Arc<RwLock<Partition>>> = (0..partition_count)
            .map(|p| Arc::new(RwLock::new(Partition::new(p))))
            .collect();
       
        let (tx, _rx) = broadcast::channel(CHANNEL_BUFF_SIZE);

        Self {
            name,
            partitions,
            subscribers:   tx,
            cur_partition: AtomicU32::new(0),
            cur_id:        AtomicU32::new(0)
        }
    }

    pub async fn publish(&self, payload: Vec<u8>) -> RafkaResult<u64> {
        let partition_id = self.get_partition();
        let message_id   = self.get_next_message_id();

        let msg = Message {
            id:        message_id,
            topic:     self.name.clone() ,
            payload,
            timestamp: global_time(),
            partition: partition_id,
        };

        let offset = {
            let mut part = self.partitions[partition_id as usize]
                .write()
                .expect("Partition lock poisoned");
          
            part.append(msg.clone())
        };

        if let Err(_) = self.subscribers.send(msg) {
            // all receivers dropped
        }
                   
        Ok(offset)
    }

    pub async fn subscribe(&self) -> broadcast::Receiver<Message> {
        self.subscribers.subscribe()        
    }

    /// Simple Round Robin
    fn get_partition(&self) -> PartitionId {
        let i = self.cur_partition.fetch_add(1, Ordering::SeqCst);
        i % self.partitions.len() as u32
    }

    fn get_next_message_id(&self) -> u64 {
        let val = self.cur_id.fetch_add(1, Ordering::SeqCst);
        val.try_into().unwrap()
    }
}

#[derive(Debug)]
pub struct Broker {
    topics:         Arc<DashMap<String, Topic>>,
    next_client_id: Arc<AtomicUsize>
}

impl Clone for Broker {
    fn clone(&self) -> Self {
        Self {
            topics:         self.topics.clone(),
            next_client_id: Arc::new(AtomicUsize::new(self.next_client_id.load(Ordering::SeqCst)))
        }
    }
}

impl Broker {
    pub fn new() -> Self {
        Self {
            topics:         Arc::new(DashMap::new()),
            next_client_id: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub async fn start(self: Arc<Self>, addr: &str) -> RafkaResult<()> {
        let listener = TcpListener::bind(addr).await
            .map_err(|e| RafkaError::Io(format!("Failed to bind to {}: {}", addr, e)))?;
        
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    let client_id = self.next_client_id.fetch_add(1, Ordering::SeqCst);
                    println!("Client {} connected from {}", client_id, addr);

                    let broker = Arc::clone(&self);

                    tokio::spawn(async move {
                        if let Err(e) = broker.handle_client(socket, client_id).await {
                            eprintln!("Client {} err: {}", client_id, e);
                        }
                        println!("Client {} disconnected", client_id);
                    });
                },
                Err(e) => {
                    println!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    async fn handle_client(
        &self,
        socket: TcpStream,
        _client_id: usize
    ) -> RafkaResult<()> {
        let (mut reader, mut writer) = socket.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<RafkaResponse>();
       
        tokio::spawn(async move {
            while let Some(resp) = rx.recv().await {
                if let Err(e) = Self::send_response(&mut writer, &resp).await {
                    eprintln!("Failed to send client response: {}", e);
                    break;
                }
            }
        });

        let mut read_buf = [0u8; 4];

        loop {
            if reader.read_exact(&mut read_buf).await.is_err() {
                break;
            }
            
            let len = u32::from_be_bytes(read_buf);

            let mut payload = vec![0u8; len as usize];

            if reader.read_exact(&mut payload).await.is_err() {
                break;
            }
                      
            let archived = access::<Archived<RafkaCommand>, Error>(&payload)
                .map_err(|e| RafkaError::Deserialization(e.to_string()))?;

            let command = deserialize::<RafkaCommand, Error>(archived)
                .map_err(|e| RafkaError::Deserialization(e.to_string()))?;

            println!("{:?}", command);
        
            match command {
                RafkaCommand::Publish {
                    topic,
                    payload
                } => {
                    if let Some(topic) = self.topics.get(&topic) {
                        println!("published");
                        topic.publish(payload).await?;
                        tx.send(RafkaResponse::Ok).unwrap();
                    } else {
                        tx.send(RafkaResponse::Error(format!("Topic '{}' not found", topic))).unwrap();
                    }
                },
                RafkaCommand::Subscribe {
                    topic
                } => {
                    if let Some(target_topic) = self.topics.get(&topic) {
                        let mut rx: broadcast::Receiver<Message> = target_topic.subscribe().await;

                        tx.send(RafkaResponse::Subscribed { topic }).unwrap();

                        let tx2 = tx.clone();

                        tokio::spawn(async move {
                            loop {
                                match rx.recv().await {
                                    Ok(msg) =>  {
                                        println!("Subscriber got message: {:?}", msg);                                        
                                        tx2.send(RafkaResponse::Message(msg)).unwrap();
                                    },
                                    Err(broadcast::error::RecvError::Lagged(_)) => {
                                        continue;
                                    },
                                    Err(broadcast::error::RecvError::Closed) => {
                                        break;
                                    }
                                }
                            }
                        });                       
                    } else {
                        tx.send(RafkaResponse::Error(format!("Topic '{}' not found", topic))).unwrap();
                    }
                },
                RafkaCommand::Unsubscribe {
                    topic: _
                } => {
                    todo!();
                },
                RafkaCommand::CreateTopic {
                    topic,
                    partition_count
                } => {
                    let new_topic: Topic = Topic::new(topic.clone(), partition_count);
                    self.topics.insert(topic, new_topic);

                    tx.send(RafkaResponse::Ok).unwrap()
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
        }
        Ok(())
    }

    async fn send_response(
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        response: &RafkaResponse,
    ) -> RafkaResult<()> {

        let bytes = rkyv::to_bytes::<Error>(response)
            .map_err(|e| RafkaError::Serialization(e.to_string()))?;

        // not safe but idgaf
        let _ = writer.write_u32(bytes.len() as u32).await;
        let _ = writer.write_all(&bytes).await;
        let _ = writer.flush().await;

        Ok(())
   }
}
