#![no_main]
#[macro_use] extern crate libfuzzer_sys;
extern crate libp2p_gossipsub;
pub use libp2p_gossipsub::*;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here

    libp2p_gossipsub::proto_to_message(data);

});
