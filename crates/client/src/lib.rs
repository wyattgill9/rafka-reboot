#![allow(dead_code)]

use tokio::net::{TcpSocket, TcpStream};
use std::io;

/// API
// $ cargo install rafka

// $ rafka server start

 // use rafka::client::Client;

 // #[tokio::main]
 // async fn main() -> Result<()> {
 //     let client = Client::connect("localhost:8080").await?;  

 //     // Publish a message
 //     client.publish("my-topic", "Hello, Rafka!").await?;

 //     // Subscribe to messages
 //     let mut subscriber = client.subscribe("my-topic").await?;
 //     while let Some(msg) = subscriber.next().await {
 //         println!("Received: {}", msg);
 //     }
 // }

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub async fn connect(addr: &str) -> io::Result<Self> {
        let socket = TcpSocket::new_v4()?;
        let stream = socket.connect(addr.parse().unwrap()).await?;

        Ok(Client { stream })
    }

    pub fn publish(&self, topic: &str, message: &str) {
        
    }

    pub fn subscribe(&self, topic: &str) {}
}
