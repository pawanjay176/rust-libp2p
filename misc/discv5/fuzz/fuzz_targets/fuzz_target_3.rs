#![no_main]
#[macro_use] extern crate libfuzzer_sys;
extern crate libp2p_discv5;
use libp2p_discv5::rpc::ProtocolMessage;


fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here

    ProtocolMessage::decode(data.to_vec());
});
