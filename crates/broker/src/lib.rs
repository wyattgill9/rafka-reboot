// TODO move to u64 as usize is inconsitent on ALL systems

// #![allow(dead_code, unused_mut)]

use std::{
    collections::VecDeque,
    sync::{atomic::{AtomicU32, AtomicUsize, Ordering}, mpsc::Receiver, Arc, RwLock}
};

use tokio::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::{mpsc, Mutex, broadcast},
    io::{AsyncWriteExt, AsyncReadExt}
};

use rkyv::{
    access, deserialize, rancor::Error,
        Archived
};

use dashmap::DashMap;

use util::{
    PartitionId,
    Message,
    RafkaCommand, RafkaResponse,
    RafkaResult, RafkaError,
    global_time
};

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
        let mut partitions: Vec<Arc<RwLock<Partition>>> = Vec::with_capacity(partition_count as usize);
        for p in 0..partition_count {
            partitions.push(Arc::new(RwLock::new(Partition::new(p))));
        }

        let (tx, _rx) = broadcast::channel(100);

        Self {
            name,
            partitions,
            subscribers:   tx,
            cur_partition: AtomicU32::new(0),
            cur_id:        AtomicU32::new(0)
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
            let mut part = self.partitions[part_idx as usize]
                .write()
                .expect("Partition lock poisoned");
      
            part.append(msg.clone());
        } // RAII to release the lock instead of calling drop(part);

        // notify all the subscribers (clientele)
        if let Err(e) = self.subscribers.send(msg) {
            eprintln!("Broadcast send error: {:?}", e); // remmeber to do debug when using eprintln!
        }
        

           
        Ok(())
    }

    pub async fn subscribe(&self) -> broadcast::Receiver<Message> {
        self.subscribers.subscribe()        
    }

    /// Simple Round Robin
    fn get_partition(&self) -> PartitionId {
        let i = self.cur_partition.fetch_add(1, Ordering::Relaxed);
        i % self.partitions.len() as u32
    }

    fn get_new_message_id(&self) -> u64 {
        let val = self.cur_id.fetch_add(1, Ordering::Relaxed);
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
        let listener = TcpListener::bind(addr).await?;
        println!("Broker on {}", addr);

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("New client connected {}", addr);

            let broker = Arc::clone(&self);
            let client_id = broker.next_client_id.fetch_add(1, Ordering::SeqCst);

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
        tokio::spawn(async move {
            while let Some(resp) = rx.recv().await {    // wait to get a responce from the reader
                if let Err(e) = Self::send_response(&mut writer, &resp).await { // send it through
                    eprintln!("Failed to send client response: {}", e);         //tcp to the client(s)
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
            reader.read_exact(&mut payload).await?;
          
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
                    if let Some(topic) = self.topics.get(&topic) { // dashmap::Ref<T> derefs to &T                    
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

                        // make a new sender
                        let tx2 = tx.clone();

                        // spawn a task to send a message every time we get one
                        tokio::spawn(async move {
                            loop {
                                match rx.recv().await {
                                    Ok(msg) =>  {
                                        println!("Subscriber got message: {:?}", msg);                                        
                                        tx2.send(RafkaResponse::Message(msg)).unwrap();
                                    },
                                    Err(broadcast::error::RecvError::Lagged(_)) => {
                                        continue; // just lagged give it a sec
                                    },
                                    Err(broadcast::error::RecvError::Closed) => {
                                        break; //real error/ connection lost
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
                                    .map(|entry| entry.key().clone()) // stirng so we gotta clone smh no deref
                                    .collect();

                    let resp = RafkaResponse::Topics(topics);
                    tx.send(resp).unwrap();
                },
            }
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

        let bytes = rkyv::to_bytes::<Error>(response)
            .map_err(|e| RafkaError::Serialization(e.to_string()))?;

        println!("{:?}", bytes);
          
        writer.write_u32(bytes.len() as u32).await?; 
        writer.write_all(&bytes).await?;
        writer.flush().await?;

        Ok(())
   }
}
