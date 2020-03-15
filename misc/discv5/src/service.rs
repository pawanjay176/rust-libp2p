//! The base UDP layer of the discv5 service.
//!
//! The `Discv5Service` opens a UDP socket and handles the encoding/decoding of raw Discv5
//! messages. These messages are defined in the `Packet` module.

use super::packet::{Packet, MAGIC_LENGTH};
use core::pin::Pin;
use futures::Future;
use log::debug;
use std::io;
use std::net::SocketAddr;
use std::task::{self, Poll};
use tokio::net::UdpSocket;
use tokio::pin;

pub(crate) const MAX_PACKET_SIZE: usize = 1280;

/// The main service that handles the transport. Specifically the UDP sockets and packet
/// encoding/decoding.
pub struct Discv5Service {
    /// The UDP socket for interacting over UDP.
    socket: UdpSocket,
    /// The buffer to accept inbound datagrams.
    recv_buffer: Box<[u8; MAX_PACKET_SIZE]>,
    /// List of discv5 packets to send.
    send_queue: Vec<(SocketAddr, Packet)>,
    /// WhoAreYou Magic Value. Used to decode raw WHOAREYOU packets.
    whoareyou_magic: [u8; MAGIC_LENGTH],
}

impl Discv5Service {
    /// Initializes the UDP socket, can fail when binding the socket.
    pub async fn new(
        socket_addr: SocketAddr,
        whoareyou_magic: [u8; MAGIC_LENGTH],
    ) -> io::Result<Self> {
        // set up the UDP socket
        let socket = UdpSocket::bind(&socket_addr).await?;

        Ok(Discv5Service {
            socket,
            recv_buffer: Box::new([0; MAX_PACKET_SIZE]),
            send_queue: Vec::new(),
            whoareyou_magic,
        })
    }

    /// Add packets to the send queue.
    pub fn send(&mut self, to: SocketAddr, packet: Packet) {
        self.send_queue.push((to, packet));
    }
}
/// Drive reading/writing to the UDP socket.
impl Future for Discv5Service {
    type Output = (SocketAddr, Packet);
    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<(SocketAddr, Packet)> {
        // let service1 = self.clone();
        let service = self.get_mut();
        // send messages
        while !service.send_queue.is_empty() {
            let (dst, packet) = service.send_queue.remove(0);

            // TODO: seems very hacky! Check if there's a better way
            let encoded = packet.encode();
            let future = service.socket.send_to(&encoded, &dst);
            pin!(future);
            match future.poll(cx) {
                Poll::Ready(Ok(bytes_written)) => {
                    debug_assert_eq!(bytes_written, packet.encode().len());
                }
                Poll::Pending => {
                    // didn't write add back and break
                    service.send_queue.insert(0, (dst, packet));
                    // notify to try again
                    cx.waker().wake_by_ref();
                    break;
                }
                Poll::Ready(Err(_)) => {
                    service.send_queue.clear();
                    break;
                }
            }
        }

        // handle incoming messages
        loop {
            // TODO: seems very hacky! Check if there's a better way and if its correct
            let mut recv_buf: Pin<_> = service.recv_buffer.clone().into();
            let mut recv_buf_mut = *recv_buf.as_mut();
            let future = service.socket.recv_from(&mut recv_buf_mut);

            pin!(future); // async functions return GenericFuture which are !Unpin

            match future.poll(cx) {
                Poll::Ready(Ok((length, src))) => {
                    let whoareyou_magic = service.whoareyou_magic;
                    let recv_buffer = *recv_buf.as_ref();
                    match Packet::decode(&recv_buffer[..length], &whoareyou_magic) {
                        Ok(p) => {
                            return Poll::Ready((src, p));
                        }
                        Err(e) => debug!("Could not decode packet: {:?}", e), // could not decode the packet, drop it
                    }
                }
                Poll::Pending => {
                    break;
                }
                Poll::Ready(Err(_)) => {
                    break;
                } // wait for reconnection to poll again.
            }
        }
        Poll::Pending
    }
}
