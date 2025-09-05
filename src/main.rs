use std::sync::Arc;
use broker::Broker;
use client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker = Arc::new(Broker::new());

    tokio::spawn(async move {
        broker.start("127.0.0.1:9000").await.unwrap();
    });
       
    let mut client = Client::connect("127.0.0.1:9000").await?;
    client.create_topic("test", 1).await?;
    
    let mut subscriber = Client::connect("127.0.0.1:9000").await?;
    subscriber.subscribe("test").await?;
        
    client.publish("test", "hello").await?;

    subscriber.receive_response().await?;   

    Ok(())
}
