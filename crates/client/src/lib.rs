#![allow(dead_code)]
/// API
// $ cargo install rafka

// $ rafka server start

//  use rafka::client::Client;

//  #[tokio::main]
//  async fn main() -> Result<()> {
//      let client = Client::connect("localhost:8080").await?;  

//      // Publish a message
//      client.publish("my-topic", "Hello, Rafka!").await?;

//      // Subscribe to messages
//      let mut subscriber = client.subscribe("my-topic").await?;
//      while let Some(msg) = subscriber.next().await {
//          println!("Received: {}", msg);
//      }
//  }

use tokio::net::{TcpSocket, TcpStream};
use std::io;

use tokio::{
    net::{TcpListener},
    sync::{mpsc, Mutex, broadcast},
    io::{AsyncWriteExt, AsyncReadExt}
};

use rkyv::{
    access, deserialize, rancor::Error, Archived, DeserializeUnsized
};

use util::{
    Message,
    RafkaCommand, RafkaResponse,
    RafkaResult, RafkaError,
};

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let socket = TcpSocket::new_v4()?;
        let stream = socket.connect(addr.parse().unwrap()).await?;

        Ok(Client { stream })
    }

    // TODO: make message a generic type T and a macro to make it into a Vec<u8>
    pub async fn publish(
        &mut self,
        topic: &str,
        message: &str
    ) -> RafkaResult<()> { // TODO: client result type dif?

        let cmd = RafkaCommand::Publish {
            topic: topic.to_string(),
            payload: message.as_bytes().to_vec(),
        };

        self.send_command(&cmd).await?;
        // self.
        Ok(())
    }

    pub async fn subscribe(
        &mut self,
        topic: &str
    ) -> RafkaResult<()> {

        let cmd = RafkaCommand::Subscribe {
            topic: topic.to_string(),
        };

        self.send_command(&cmd).await?;

        let response = self.receive_response().await?;
        match response {
            RafkaResponse::Ok => Ok(()),
            _ => {panic!()}
        }
    }

    pub async fn unsubscribe(
        &mut self,
        topic: &str
    ) -> RafkaResult<()> {
        let cmd = RafkaCommand::Unsubscribe {
            topic: topic.to_string(),
        };

        self.send_command(&cmd).await?;

        let response = self.receive_response().await?;
        match response {
            RafkaResponse::Ok => Ok(()),
            _ => {panic!()}
        }
    }        

    
    pub async fn list_topics(
        &mut self,
        topic: &str
    ) -> RafkaResult<Vec<String>> {

        let cmd = RafkaCommand::Subscribe {
            topic: topic.to_string(),
        };

        self.send_command(&cmd).await?;

        let response = self.receive_response().await?;

        match response {
            RafkaResponse::Topics(topics) => {
                Ok(topics)
            },
            _ => panic!(),
        }
    }

    async fn send_command(
        &mut self,
        response: &RafkaCommand,
    ) -> RafkaResult<()> {

        let bytes = rkyv::to_bytes::<Error>(response).unwrap();
        println!("{:?}", bytes);
          
        self.stream.write_u32(bytes.len() as u32).await?; 
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;

        Ok(())
    }

    async fn receive_response(
        &mut self,
    ) -> RafkaResult<RafkaResponse> {

        let len = self.stream.read_u32().await? as usize;
        
        let mut buffer = vec![0u8; len];
        self.stream.read_exact(&mut buffer).await?;
        
        let archived = access::<Archived<RafkaResponse>, Error>(&buffer)
            .map_err(|e| RafkaError::Deserialization(e.to_string()))?;
        
        let response = deserialize::<RafkaResponse, Error>(archived)
            .map_err(|e| RafkaError::Deserialization(e.to_string()))?;
        
        Ok(response)
    }
}
