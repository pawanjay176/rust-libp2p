#![no_main]
#[macro_use] extern crate libfuzzer_sys;
extern crate libp2p_discv5;

use libp2p_discv5::packet::Packet;

fuzz_target!(|data: &[u8]| {
    if data.len() > 32 {
        let mut magic_data = [0u8;32];
        magic_data.copy_from_slice(&data[..32]);
        Packet::decode(&data[32..], &magic_data);
    }

});
