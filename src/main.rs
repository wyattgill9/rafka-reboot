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

use cbridge::add;
use broker::Broker;

fn main() {
    let mut broker: Broker = Broker::new(0);
    broker.create_partition(String::from("Orders"), 0, true);
}
