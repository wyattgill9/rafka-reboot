use std::sync::Arc;

use broker::Broker;
use client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Arc::new(Broker::new());
    tokio::spawn(async move {
        broker.start("127.0.0.1:9000").await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut client = Client::connect("127.0.0.1:9000").await?;
    client.create_topic("hello", 1).await?;

    
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut subscriber = Client::connect("127.0.0.1:9000").await?;
    subscriber.subscribe("hello").await?;

    client.publish("hello", "yo").await?;
    Ok(())
}
