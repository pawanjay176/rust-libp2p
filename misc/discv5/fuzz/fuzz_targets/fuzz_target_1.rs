#![no_main]
#[macro_use] extern crate libfuzzer_sys;
extern crate libp2p_discv5;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
});
