//! Session management for the Discv5 Discovery service.
//!
//! The `SessionService` is responsible for establishing and maintaining sessions with
//! connected/discovered nodes. Each node, identified by it's [`NodeId`] is associated with a
//! [`Session`]. This service drives the handshakes for establishing the sessions and associated
//! logic for sending/requesting initial connections/ENR's from unknown peers.
//!
//! The `SessionService` also manages the timeouts for each request and reports back RPC failures,
//! session timeouts and received messages. Messages are enrypted and decrypted using the
//! associated `Session` for each node.
//!
//TODO: Document the Event structure and WHOAREYOU requests to the protocol layer.

use super::packet::{
    AuthHeader, AuthResponse, AuthTag, NodeId, Nonce, Packet, Tag, MAGIC_LENGTH, TAG_LENGTH,
};
use super::service::Discv5Service;
use crate::error::Discv5Error;
use crate::rpc::{ProtocolMessage, RpcType};
use crate::session::{Session, SessionStatus};
use enr::Enr;
use fnv::FnvHashMap;
use futures::prelude::*;
use hex;
use libp2p_core::identity::Keypair;
use log::{debug, error, trace, warn};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::default::Default;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};
use tokio::timer::Interval;

// TODO: Drop the session on RPC timeout.

mod tests;

/// Seconds before a timeout expires.
const REQUEST_TIMEOUT: u64 = 10;
/// The number of times to retry a request.
const REQUEST_RETRIES: u8 = 2;
/// The interval (in seconds) to check for timeouts.
const HEARTBEAT_INTERVAL: u64 = 2;

pub struct SessionService {
    /// Queue of events produced by the session service.
    events: VecDeque<SessionEvent>,

    /// The local ENR.
    enr: Enr,

    /// The keypair to sign the ENR and set up encrypted communication with peers.
    keypair: Keypair,

    /// Pending raw requests. A list of raw messages we are awaiting a response from the remote
    /// for.
    pending_requests: FnvHashMap<AuthTag, Request>,

    /// Sent WHOAREYOU messages. Stored separately to lookup via `NodeId`.
    whoareyou_requests: FnvHashMap<NodeId, Request>,

    /// Pending messages. Messages awaiting to be sent, once a handshake has been established.
    pending_messages: FnvHashMap<NodeId, Vec<ProtocolMessage>>,

    /// Sessions that have been created for each node id. These can be established or
    /// awaiting response from remote nodes.
    sessions: FnvHashMap<NodeId, Session>,

    /// Heartbeat timer, used to to check for message and session timeouts.
    heartbeat: Interval,

    /// The discovery v5 UDP service.
    service: Discv5Service,
}

impl SessionService {
    /// A new Session service which instantiates the UDP socket.
    pub fn new(enr: Enr, keypair: Keypair) -> io::Result<Self> {
        // ensure the keypair matches the one that signed the enr.
        if enr.public_key().into_protobuf_encoding() != keypair.public().into_protobuf_encoding() {
            panic!("Discv5: Provided keypair does not match the provided ENR keypair");
        }

        let udp = enr.udp().unwrap_or_else(|| 9000);
        let ip = enr
            .ip()
            .unwrap_or_else(|| "127.0.0.1".parse().expect("valid ip"));

        let socket_addr = SocketAddr::new(ip, udp);
        // generates the WHOAREYOU magic packet for the local node-id
        let magic = {
            let mut hasher = Sha256::new();
            hasher.input(enr.node_id);
            hasher.input(b"WHOAREYOU");
            let mut magic = [0u8; MAGIC_LENGTH];
            magic.copy_from_slice(&hasher.result());
            magic
        };

        Ok(SessionService {
            events: VecDeque::new(),
            enr,
            keypair,
            pending_requests: FnvHashMap::default(),
            whoareyou_requests: FnvHashMap::default(),
            pending_messages: FnvHashMap::default(),
            sessions: FnvHashMap::default(),
            heartbeat: Interval::new(
                Instant::now() + Duration::from_secs(HEARTBEAT_INTERVAL),
                Duration::from_secs(HEARTBEAT_INTERVAL),
            ),
            service: Discv5Service::new(socket_addr, magic)?,
        })
    }

    /// Sends a message to a node given the node's ENR. This function will handle establishing a
    /// session and retrying requests on timeout. An optional SocketAddr can be provided to
    /// override sending the packet based on the node's ENR.
    pub fn send_message(
        &mut self,
        dst_enr: &Enr,
        message: ProtocolMessage,
        socket_addr: Option<SocketAddr>,
    ) -> Result<(), Discv5Error> {
        // check for an established session
        let dst_id = dst_enr.node_id;

        let dst = {
            if let Some(socket) = socket_addr {
                socket
            } else {
                dst_enr.udp_socket().ok_or_else(|| {
                    warn!(
                        "Could not send message. ENR has no ip and udp port: {}",
                        dst_enr
                    );
                    Discv5Error::InvalidEnr
                })?
            }
        };

        let session = match self.sessions.get(&dst_id) {
            Some(s) if s.established() => s,
            Some(_) => {
                // we are currently establishing a connection, add to pending messages
                debug!("Awaiting a session to established, caching message");
                let msgs = self
                    .pending_messages
                    .entry(dst_id)
                    .or_insert_with(|| Vec::new());
                msgs.push(message);
                return Ok(());
            }
            None => {
                debug!(
                    "No session established, sending a random packet to: {}",
                    hex::encode(dst_id)
                );
                // cache message
                let msgs = self
                    .pending_messages
                    .entry(dst_id)
                    .or_insert_with(|| Vec::new());
                msgs.push(message);

                // need to establish a new session, send a random packet
                let (session, packet) = Session::new_random(self.tag(&dst_id), dst_enr.clone());
                self.send_request_packet(dst, &dst_id, packet, None);
                self.sessions.insert(dst_id, session);
                return Ok(());
            }
        };

        // avoid cloning a message
        let id = message.id;
        let mut is_request = false;
        if let RpcType::Request(_) = &message.body {
            is_request = true;
        }

        // session is established, encrypt the message and send
        let packet = session
            .encrypt_message(self.tag(&dst_id), &message.encode())
            .map_err(|e| {
                error!("Failed to encrypt message");
                e
            })?;

        // only log the packet request if it's an RPC request
        if is_request {
            self.send_request_packet(dst, &dst_id, packet, Some(id));
        } else {
            // send the response
            self.service.send(dst, packet);
        }

        Ok(())
    }

    /// This is called in response to a SessionMessage::WhoAreYou event. The protocol finds the
    /// highest known ENR then calls this function to send a WHOAREYOU packet.
    //TODO: Comment why remote_enr
    //TODO: Create a more elegant API
    pub fn send_whoareyou(
        &mut self,
        dst: SocketAddr,
        node_id: &NodeId,
        enr_seq: u64,
        remote_enr: Option<Enr>,
        auth_tag: AuthTag,
    ) {
        // If a WHOAREYOU is already sent or a session is already established, ignore this request.
        match self.sessions.get(node_id) {
            Some(s) if s.established() || s.status() == SessionStatus::WhoAreYouSent => {
                warn!("Session exits. WhoAreYou packet not sent");
                return;
            }
            _ => {}
        }

        debug!("Sending WHOAREYOU packet to: {}", hex::encode(node_id));
        let (session, packet) =
            Session::new_whoareyou(self.tag(node_id), node_id, enr_seq, remote_enr, auth_tag);
        self.sessions.insert(node_id.clone(), session);
        self.send_request_packet(dst, node_id, packet, None);
    }

    /// Calculates the src `NodeId` given a tag.
    fn src_id(&self, tag: &Tag) -> Tag {
        let hash = Sha256::digest(&self.enr.node_id);
        let mut src_id: Tag = Default::default();
        for i in 0..TAG_LENGTH {
            src_id[i] = hash[i] ^ tag[i];
        }
        src_id
    }

    /// Calculates the tag given a `NodeId`.
    fn tag(&self, dst_id: &NodeId) -> Tag {
        let hash = Sha256::digest(dst_id);
        let mut tag: Tag = Default::default();
        for i in 0..TAG_LENGTH {
            tag[i] = hash[i] ^ self.enr.node_id[i];
        }
        tag
    }

    /// Handles a WHOAREYOU packet that was received from the network.
    fn handle_whoareyou(
        &mut self,
        src: SocketAddr,
        src_id: NodeId,
        token: AuthTag,
        id_nonce: Nonce,
        enr_seq: u64,
    ) -> Result<(), ()> {
        // the auth-tag must match a pending request
        let req = self.pending_requests.remove(&token).ok_or_else(|| {
            debug!("Received a WHOAREYOU packet that references an unknown or expired request")
        })?;

        // the referenced request must come from the expected src or node-id
        if src != req.dst || src_id != req.node_id {
            // add the request back
            self.pending_requests.insert(token, req);
            warn!("Incorrect WHOAREYOU packet source");
            return Err(());
        }

        debug!("Received a WHOAREYOU packet from: {}", hex::encode(src_id));

        let tag = self.tag(&src_id);

        // find the session associated with this WHOAREYOU
        let session = self.sessions.get_mut(&src_id).ok_or_else(|| {
            error!("Received a WHOAREYOU packet without having an established session")
        })?;

        // get the messages that are waiting for an established session
        let messages = self
            .pending_messages
            .get_mut(&src_id)
            .ok_or_else(|| error!("No pending messages found for WHOAREYOU request."))?;

        if messages.is_empty() {
            debug!("No pending messages found for WHOAREYOU request.");
            return Err(());
        }

        // sign the nonce
        let nonce = Session::generate_nonce(id_nonce);
        let sig = self.keypair.sign(&nonce).map_err(|e| {
            error!(
                "Error signing WHOAREYOU Nonce. Ignoring  WHOAREYOU packet. Error: {:?}",
                e
            )
        })?;

        // update the enr record if we need need to
        let mut updated_enr = None;
        if enr_seq < self.enr.seq {
            updated_enr = Some(self.enr.clone());
        }

        // generate the auth response to be encrypted
        let auth_pt = AuthResponse::new(&sig, updated_enr).encode();

        // encrypt the earliest message
        let earliest_message = messages.remove(0);
        let rpc_id = earliest_message.id;

        // generate session keys and encrypt the earliest packet with the authentication header
        let auth_packet = session
            .encrypt_with_header(
                tag,
                &self.enr.node_id,
                &id_nonce,
                &auth_pt,
                &earliest_message.clone().encode(),
            )
            .map_err(|e| {
                // insert the message back into the queue
                messages.insert(0, earliest_message);
                error!("Could not generate a session. Error: {:?}", e)
            })?;

        debug!("Session established with node: {}", hex::encode(src_id));
        // send the response
        debug!(
            "Sending Authentication response to node: {} at: {:?}",
            hex::encode(src_id),
            src
        );
        self.send_request_packet(src, &src_id, auth_packet, Some(rpc_id));

        // flush the message cache
        let _ = self.flush_messages(src, &src_id);
        Ok(())
    }

    /// Handle a message that contains an authentication header.
    fn handle_auth_message(
        &mut self,
        src: SocketAddr,
        tag: Tag,
        auth_header: AuthHeader,
        message: &[u8],
    ) -> Result<(), ()> {
        // Needs to match an outgoing WHOAREYOU packet (so we have the required nonce to be signed). If it doesn't we drop the packet. This will
        // lead to future outgoing WHOAREYOU packets if they proceed to send further encrypted
        // packets.
        let src_id = self.src_id(&tag);
        debug!(
            "Received an Authentication header message from: {}",
            hex::encode(src_id)
        );

        let session = self.sessions.get_mut(&src_id).ok_or_else(|| {
            warn!("Received an authenticated header without a known session. Dropping")
        })?;

        // check that we are awaiting a response for a WHOAREYOU message
        if session.status() != SessionStatus::WhoAreYouSent {
            warn!("Received an authenticated header without a known WHOAREYOU session. Dropping");
            return Err(());
        }

        let req = self
            .whoareyou_requests
            .remove(&src_id)
            .ok_or_else(|| error!("There was no WHOAREYOU request associated with a session"))?;

        // verify the source ip, avoid spam and signature verification calculations from malicious packets.
        // the referenced request must come from the expected src or node-id
        if src != req.dst || src_id != req.node_id {
            warn!("Received an authenticated header from incorrect source. Expected id: {:?}, actual id: {:?}, expected source: {:?} actual source: {:?}", req.node_id, src_id, req.dst, src);
            // add the request back
            self.whoareyou_requests.insert(src_id, req);
            return Err(());
        }

        // get the nonce
        let id_nonce = match req.packet {
            Packet::WhoAreYou { id_nonce, .. } => id_nonce,
            _ => unreachable!("Coding error if there is not a WHOAREYOU packet in this request"),
        };

        let updated_enr = session
            .generate_keys_from_header(
                tag,
                &self.keypair,
                &self.enr.node_id,
                id_nonce,
                &auth_header,
            )
            .map_err(|e| warn!("Invalid Authentication header: {:?}", e))?;

        // verify the updated_enr. Ensure that if an ENR is required for this session, that it is
        // provided

        // Verify the ENR update
        if let Some(enr) = updated_enr {
            if let Some(remote_enr) = session.remote_enr() {
                // verify the enr-seq number
                if remote_enr.seq < enr.seq {
                    session.update_enr(enr.clone());
                    self.events
                        .push_back(SessionEvent::UpdatedEnr(Box::new(enr)));
                } // ignore ENR's that have a lower seq number
            } else if session.remote_enr().is_none() {
                // update the ENR
                session.update_enr(enr.clone());
                self.events
                    .push_back(SessionEvent::UpdatedEnr(Box::new(enr)));
            }
        } else if session.remote_enr().is_none() {
            // a required ENR was not provided
            // drop the session and exit.
            warn!(
                "Node did not provide a required ENR. Dropping session. Node: {:?}",
                src_id
            );
            self.sessions.remove(&src_id);
            return Err(());
        }

        // decrypt the message
        let mut aad = tag.to_vec();
        aad.append(&mut auth_header.encode());
        // log and continue on error
        let _ = self.handle_message(src, src_id, auth_header.auth_tag, message, &aad);

        // flush messages awaiting a session
        let _ = self.flush_messages(src, &src_id);
        Ok(())
    }

    /// Handle a standard message that does not contain an authentication header.
    fn handle_message(
        &mut self,
        src: SocketAddr,
        src_id: NodeId,
        auth_tag: AuthTag,
        message: &[u8],
        aad: &[u8],
    ) -> Result<(), ()> {
        // check if we have an established session

        let session = match self.sessions.get(&src_id) {
            Some(s) if s.established() => s,
            Some(s) => {
                // if we have sent a random packet, upgrade to a WhoAreYou request
                if s.status() == SessionStatus::RandomSent {
                    let event = SessionEvent::WhoAreYouRequest {
                        src,
                        src_id: src_id.clone(),
                        auth_tag,
                    };
                    self.events.push_back(event);
                } else {
                    debug!("Waiting for a session to be generated.");
                    // potentially store and decrypt once we receive the packet.
                    // drop it for now.
                }
                return Ok(());
            }
            None => {
                // no session exists
                debug!("Received a message without a session.");
                debug!("Requesting a WHOAREYOU packet to be sent");
                // spawn a WHOAREYOU event to check for highest known ENR
                let event = SessionEvent::WhoAreYouRequest {
                    src,
                    src_id: src_id.clone(),
                    auth_tag,
                };
                self.events.push_back(event);
                return Ok(());
            }
        };

        // we have a known session, decrypt and process the message
        let message = match session.decrypt_message(auth_tag, message, aad) {
            Ok(m) => ProtocolMessage::decode(m)
                .map_err(|e| warn!("Failed to decode message. Error: {:?}", e))?,
            Err(e) => {
                debug!("Message from node: {:?} in not encrypted with known session keys. Requesting WHOAREYOU packet. Error: {:?}", hex::encode(src_id), e);
                // spawn a WHOAREYOU event to check for highest known ENR
                let event = SessionEvent::WhoAreYouRequest {
                    src,
                    src_id: src_id.clone(),
                    auth_tag,
                };
                self.events.push_back(event);
                return Err(());
            }
        };

        // we have received a new message. Notify the behaviour.
        debug!("Message received: {:?}", message);
        let event = SessionEvent::Message {
            src_id,
            src,
            message: Box::new(message),
        };
        self.events.push_back(event);
        Ok(())
    }

    /// Encrypts and sends any messages that were waiting for a session to be established.
    #[inline]
    fn flush_messages(&mut self, dst: SocketAddr, dst_id: &NodeId) -> Result<(), ()> {
        let mut packets_to_send = Vec::new();

        {
            // get the session for this id
            let session = match self.sessions.get(dst_id) {
                Some(s) if s.established() => s,
                _ => {
                    // no session
                    debug_assert!(false);
                    return Err(());
                }
            };

            let tag = self.tag(&dst_id);

            let mut messages = self
                .pending_messages
                .remove(dst_id)
                .ok_or_else(|| trace!("No messages to send"))?;

            for _ in 0..messages.len() {
                let msg = messages.remove(0);
                let rpc_id = msg.id;
                let packet = session
                    .encrypt_message(tag, &msg.encode())
                    .map_err(|e| warn!("Failed to encrypt message, Error: {:?}", e))?;
                packets_to_send.push((packet, rpc_id));
            }
        }

        for (packet, rpc_id) in packets_to_send.into_iter() {
            debug!("Sending cached message");
            self.send_request_packet(dst, dst_id, packet, Some(rpc_id));
        }
        Ok(())
    }

    /// Wrapper around `service.send()` that adds all sent messages to the `pending_requests`.
    #[inline]
    fn send_request_packet(
        &mut self,
        dst: SocketAddr,
        node_id: &NodeId,
        packet: Packet,
        rpc_id: Option<u64>,
    ) {
        self.service.send(dst, packet.clone());
        let request = Request {
            rpc_id,
            dst,
            node_id: node_id.clone(),
            packet,
            timeout: Instant::now() + Duration::from_secs(REQUEST_TIMEOUT),
            retries: 1,
        };

        match &request.packet {
            Packet::WhoAreYou { .. } => {
                self.whoareyou_requests.insert(node_id.clone(), request);
            }
            _ => {
                let auth_tag = request
                    .packet
                    .auth_tag()
                    .expect("Must have an auth_tag")
                    .clone();
                self.pending_requests.insert(auth_tag, request);
            }
        }
    }

    /// The heartbeat which checks for timeouts and reports back failed RPC requests/sessions.
    fn heartbeat(&mut self) {
        // remove expired requests/sessions
        // log pending request timeouts
        let now = Instant::now();
        for (_, req) in self.pending_requests.iter() {
            if req.timeout < now && req.retries >= REQUEST_RETRIES {
                // determine which kind of RPC has timed out
                let node_id = req.node_id;
                match req.packet {
                    Packet::RandomPacket { .. } => {
                        // no response from peer, flush all pending messages
                        if let Some(pending_messages) = self.pending_messages.remove(&req.node_id) {
                            for msg in pending_messages {
                                self.events.push_back(SessionEvent::RequestFailed(
                                    node_id.clone(),
                                    msg.id,
                                ));
                            }
                        }
                    }
                    Packet::AuthMessage { .. } | Packet::Message { .. } => {
                        self.events.push_back(SessionEvent::RequestFailed(
                            node_id,
                            req.rpc_id.expect("Auth messages have an rpc id"),
                        ));
                    }
                    Packet::WhoAreYou { .. } => {
                        unreachable!("WHOAREYOU requests are not in requests")
                    }
                }
            }
        }

        // log WHOAREYOU request timeouts
        for (node_id, req) in self.whoareyou_requests.iter() {
            if req.timeout < now && req.retries >= REQUEST_RETRIES {
                if let Some(pending_messages) = self.pending_messages.remove(node_id) {
                    for msg in pending_messages {
                        self.events
                            .push_back(SessionEvent::RequestFailed(node_id.clone(), msg.id));
                    }
                }
            }
        }

        // remove the entries from their mappings
        self.pending_requests
            .retain(|_, req| req.timeout >= now || req.retries < REQUEST_RETRIES);
        // remove expired WHOAREYOU requests
        self.whoareyou_requests
            .retain(|_, req| req.timeout >= now || req.retries < REQUEST_RETRIES);

        /*
        for (node_id, session) in self.sessions.iter() {
            if !session.timeout().map_or(true, |t| t<= now) {
                debug!("Session timed out for node: {:?}", node_id);
            }
        }
        */

        // remove timed-out sessions
        self.sessions.retain(|node_id, session| {
            session.timeout().map_or(true, {
                debug!("Session timed out for node: {:?}", node_id);
                |t| t <= now
            })
        });

        // resend requests
        for req in self
            .pending_requests
            .values_mut()
            .chain(self.whoareyou_requests.values_mut())
            .filter(|v| v.timeout < Instant::now())
        {
            debug!("Resending message to {:?}", hex::encode(req.node_id));
            self.service.send(req.dst, req.packet.clone());
            req.retries += 1;
        }
    }

    pub fn poll(&mut self) -> Async<SessionEvent> {
        // process any events if necessary
        if let Some(event) = self.events.pop_front() {
            return Async::Ready(event);
        }

        // poll the discv5 service
        loop {
            match self.service.poll() {
                Async::Ready((src, packet)) => {
                    match packet {
                        Packet::WhoAreYou {
                            tag,
                            token,
                            id_nonce,
                            enr_seq,
                            ..
                        } => {
                            let src_id = self.src_id(&tag);
                            self.handle_whoareyou(src, src_id, token, id_nonce, enr_seq);
                        }
                        Packet::AuthMessage {
                            tag,
                            auth_header,
                            message,
                        } => {
                            self.handle_auth_message(src, tag, auth_header, &message);
                        }
                        Packet::Message {
                            tag,
                            auth_tag,
                            message,
                        } => {
                            let src_id = self.src_id(&tag);
                            self.handle_message(src, src_id, auth_tag, &message, &tag);
                        }
                        Packet::RandomPacket { .. } => {} // this will not be decoded.
                    }
                }
                Async::NotReady => break,
            }
        }

        // check for timeouts
        loop {
            match self.heartbeat.poll() {
                Ok(Async::Ready(_)) => {
                    self.heartbeat();
                }
                Ok(Async::NotReady) => {
                    break;
                }
                _ => {
                    panic!("Discv5 heartbeat has ended");
                }
            }
        }

        Async::NotReady
    }
}

#[derive(Debug)]
/// The output from polling the `SessionSerivce`.
pub enum SessionEvent {
    /// A message was received.
    Message {
        src_id: NodeId,
        src: SocketAddr,
        message: Box<ProtocolMessage>,
    },

    /// An ENR was updated for a node.
    UpdatedEnr(Box<Enr>),

    /// A WHOAREYOU packet needs to be sent. This requests the protocol layer to send back the
    /// highest known ENR.
    WhoAreYouRequest {
        src: SocketAddr,
        src_id: NodeId,
        auth_tag: AuthTag,
    },

    /// An RPC request failed. The parameters are NodeId and the RPC-ID associated with the
    /// request.
    RequestFailed(NodeId, u64),
}

#[derive(Debug, Clone)]
/// A request to a node that we are waiting for a response.
pub struct Request {
    /// The Id associated with the RPC.
    pub rpc_id: Option<u64>,

    /// The destination socket address.
    pub dst: SocketAddr,

    /// The `NodeId` the request was sent to.
    pub node_id: NodeId,

    /// The raw discv5 packet sent.
    pub packet: Packet,

    /// The `Instant` when this request timesout.
    pub timeout: Instant,

    /// The number of times to re-send this request until it is considered failed.
    pub retries: u8,
}
