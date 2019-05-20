//! This starts a discv5 UDP service connection which handles discovery of peers and manages topics
//! and their advertisements as described by the [discv5
//! specification](https://github.com/ethereum/devp2p/blob/master/discv5/discv5.md).

use super::packet::{Packet, MAGIC_LENGTH};
use futures::prelude::*;
// use sha2::{Digest, Sha256};
use std::io;
use std::net::SocketAddr;
// use std::time::Instant;
use tokio_udp::UdpSocket;

const MAX_PACKET_SIZE: usize = 1280;

/// The main service that handles the transport. Specifically the UDP sockets and session/encryption.
pub struct Discv5Service {
    /// The UDP socket for interacting over UDP.
    socket: UdpSocket,
    /// The buffer to accept inbound datagrams.
    recv_buffer: [u8; MAX_PACKET_SIZE],
    /// List of discv5 packets to send.
    send_queue: Vec<(SocketAddr, Packet)>,
    /// WhoAreYou Magic Value. Used to decode raw WHOAREYOU packets.
    whoareyou_magic: [u8; MAGIC_LENGTH],
}

impl Discv5Service {
    pub fn new(socket_addr: SocketAddr, whoareyou_magic: [u8; MAGIC_LENGTH]) -> io::Result<Self> {
        // set up the UDP socket
        let socket = UdpSocket::bind(&socket_addr)?;

        Ok(Discv5Service {
            socket,
            recv_buffer: [0; MAX_PACKET_SIZE],
            send_queue: Vec::new(),
            whoareyou_magic,
        })
    }

    /// Add packets to the send queue.
    pub fn send(&mut self, to: SocketAddr, packet: Packet) {
        self.send_queue.push((to, packet));
    }

    pub fn poll(&mut self) -> Async<(SocketAddr, Packet)> {
        // query

        // send

        // handle incoming messages
        loop {
            match self.socket.poll_recv_from(&mut self.recv_buffer) {
                Ok(Async::Ready((length, src))) => {
                    match Packet::decode(&self.recv_buffer[..length], &self.whoareyou_magic) {
                        Ok(p) => {
                            return Async::Ready((src, p));
                        }
                        Err(_) => {} // could not decode the packet, drop it
                    }
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Err(_) => {
                    break;
                } // wait for reconnection to poll again.
            }
        }
        Async::NotReady
    }
}