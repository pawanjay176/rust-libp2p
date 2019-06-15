#![no_main]
#[macro_use] extern crate libfuzzer_sys;
extern crate libp2p_gossipsub;
pub use libp2p_gossipsub::protocol;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here

    protocol::proto_to_message(data);

});
