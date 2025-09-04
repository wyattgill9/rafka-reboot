#![allow(dead_code)]

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

    pub async fn create_topic(
        &mut self,
        topic: &str,
        partition_count: u32
    ) -> RafkaResult<()> {
        let cmd = RafkaCommand::CreateTopic {
            topic: topic.to_string(),
            partition_count
        };

        self.send_command(&cmd).await?;
        Ok(())
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
        match self.receive_response().await {
            Ok(RafkaResponse::Ok) => { Ok(()) }
            _ => { panic!(); }          
        }
    }

    pub async fn subscribe(&mut self, topic: &str) -> RafkaResult<()> {               
        let cmd = RafkaCommand::Subscribe {
            topic: topic.to_string(),
        };

        self.send_command(&cmd).await?;

        let response = self.receive_response().await?;
        match response {
            RafkaResponse::Subscribed { topic: t } if t == topic => Ok(()),
            RafkaResponse::Error(e) => Err(RafkaError::Deserialization(e)),
            _ => Err(RafkaError::Deserialization("unexpected response to subscribe".to_string())),
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
            _ => { panic!() }
        }
    }        

    
    pub async fn list_topics(
        &mut self,
    ) -> RafkaResult<Vec<String>> {

        let cmd = RafkaCommand::ListTopics;

        self.send_command(&cmd).await?;

        let response = self.receive_response().await;

        match response {
            Ok(RafkaResponse::Topics(topics)) => { return Ok(topics) }
            _ => { panic!() }
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

