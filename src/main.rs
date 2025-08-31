#![allow(unused_imports)]

/// TODO LIST:
// SINGLE-BROKER (SINGLE NODE):
// - RUST: Raft Algorithm, crates/raft
//   - pub trait RaftNode, which broker implements
// - C: Efficient Message Protocol over TCP, that will be used for
//   - Producer(s) -> [Broker(s) <-> Broker(s)] -> Consumer(s)
// - RUST: Broker implementation
// - RUST: Producer API
// - RUST: Consumer API

// MULI-BROKER (DISTRIBUTED):
// TODO

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

/// MAIN

use cbridge::add;
use client::Client;

fn main() {}
