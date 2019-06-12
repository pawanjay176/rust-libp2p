//! This module defines the raw UDP message packets for Discovery v5.
//!
//! The [discv5 wire specification](https://github.com/ethereum/devp2p/blob/master/discv5/discv5.md) provides
//! further information on UDP message packets as implemented in this module.
//!
//! The `Packet` struct defines all raw UDP message variants and implements the encoding/decoding
//! logic.
//!
//! Note, that all message encryption/decryption is handled outside of this module.

mod auth_header;

pub use auth_header::AuthHeader;
pub use auth_header::AuthResponse;
use log::debug;
use rlp::Decodable;
use std::default::Default;

pub const NODE_ID_LENGTH: usize = 32;
pub const TAG_LENGTH: usize = 32;
const AUTH_TAG_LENGTH: usize = 12;
pub const MAGIC_LENGTH: usize = 32;
pub const ID_NONCE_LENGTH: usize = 32;

/// The authentication nonce (12 bytes).
pub type AuthTag = [u8; AUTH_TAG_LENGTH];
/// Packet Tag
pub type Tag = [u8; TAG_LENGTH];
/// ENR NodeId.
pub type NodeId = [u8; NODE_ID_LENGTH];
/// The nonce sent in a WHOAREYOU packet.
pub type Nonce = [u8; ID_NONCE_LENGTH];

#[derive(Debug, Clone, PartialEq)]
pub enum Packet {
    /// Packet for establishing handshake.
    RandomPacket {
        /// The XOR(SHA256(dest-node-id), src-node-id).
        tag: Tag,

        /// Random auth_tag formatted as rlp_bytes(bytes).
        auth_tag: AuthTag,

        /// At least 44 bytes of random data.
        data: Box<Vec<u8>>,
    },
    /// Handshake packet to establish identities.
    WhoAreYou {
        /// The XOR(SHA256(dest-node-id), src-node-id).
        tag: Tag,

        /// SHA256(`dest-node-id` || "WHOAREYOU").
        magic: [u8; MAGIC_LENGTH],

        /// The auth-tag of the request.
        token: AuthTag, //potentially rename to auth-tag

        /// The `id-nonce` to prevent handshake replays.
        id_nonce: Nonce,

        /// Highest known ENR sequence number of node.
        enr_seq: u64,
    },
    /// Message sent with an extended authentication header.
    AuthMessage {
        /// The XOR(SHA256(dest-node-id), src-node-id).
        tag: Tag,

        /// Authentication header.
        auth_header: AuthHeader,

        /// The encrypted message including the authentication header.
        message: Box<Vec<u8>>,
    },
    /// A standard discv5 message.
    Message {
        /// The XOR(SHA256(dest-node-id), src-node-id).
        tag: Tag,

        /// 12 byte Authentication nonce.
        auth_tag: AuthTag,

        /// The encrypted message as raw bytes.
        message: Box<Vec<u8>>,
    },
}

impl Packet {
    /// Generates a Packet::Random given a `tag`.
    pub fn random(tag: Tag) -> Packet {
        let data: Vec<u8> = (0..44).map(|_| rand::random::<u8>()).collect();
        let data = Box::new(data);

        Packet::RandomPacket {
            tag,
            auth_tag: rand::random(),
            data,
        }
    }

    /// The authentication tag for all packets except WHOAREYOU.
    pub fn auth_tag(&self) -> Option<&AuthTag> {
        match &self {
            Packet::RandomPacket { auth_tag, .. } => Some(auth_tag),
            Packet::AuthMessage { auth_header, .. } => Some(&auth_header.auth_tag),
            Packet::Message { auth_tag, .. } => Some(auth_tag),
            Packet::WhoAreYou { .. } => None,
        }
    }

    /// Encodes a packet to bytes.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Packet::RandomPacket {
                tag,
                auth_tag,
                data,
            } => {
                let mut buf = Vec::with_capacity(TAG_LENGTH + AUTH_TAG_LENGTH + 1 + 44); // at least 44 random bytes
                buf.extend_from_slice(tag);
                buf.extend_from_slice(&rlp::encode(&auth_tag.to_vec()));
                buf.extend_from_slice(&data);
                buf
            }
            Packet::WhoAreYou {
                tag,
                magic,
                token,
                id_nonce,
                enr_seq,
            } => {
                let mut buf = Vec::with_capacity(
                    TAG_LENGTH + MAGIC_LENGTH + AUTH_TAG_LENGTH + ID_NONCE_LENGTH + 8 + 2,
                ); // + enr + rlp
                buf.extend_from_slice(tag);
                buf.extend_from_slice(magic);
                let list = rlp::encode_list::<Vec<u8>, Vec<u8>>(&[
                    token.to_vec(),
                    id_nonce.to_vec(),
                    enr_seq.to_be_bytes().to_vec(),
                ]);
                buf.extend_from_slice(&list);
                buf
            }
            Packet::AuthMessage {
                tag,
                auth_header,
                message,
            } => {
                let mut buf = Vec::with_capacity(TAG_LENGTH + 60); // TODO: Estimate correctly
                buf.extend_from_slice(tag);
                buf.extend_from_slice(&rlp::encode(auth_header));
                buf.extend_from_slice(&message.to_vec());
                buf
            }
            Packet::Message {
                tag,
                auth_tag,
                message,
            } => {
                let mut buf = Vec::with_capacity(TAG_LENGTH + AUTH_TAG_LENGTH + 1 + 24);
                buf.extend_from_slice(tag);
                buf.extend_from_slice(&rlp::encode(&auth_tag.to_vec()));
                buf.extend_from_slice(&message.to_vec());
                buf
            }
        }
    }

    /// Decodes a WHOAREYOU packet.
    fn decode_whoareyou(tag: Tag, data: &[u8]) -> Result<Self, PacketError> {
        // 32 tag + 32 magic + 32 token + 12 id + 2 enr + 1 rlp
        // decode the rlp list
        let rlp_list = data[TAG_LENGTH + MAGIC_LENGTH..].to_vec();
        let rlp = rlp::Rlp::new(&rlp_list);
        let mut decoded_list = match rlp.as_list::<Vec<u8>>() {
            Ok(v) => v,
            Err(_) => {
                debug!("Could not decode WHOAREYOU packet: {:?}", data);
                return Err(PacketError::UnknownFormat);
            }
        };
        // build objects
        let mut magic: [u8; MAGIC_LENGTH] = Default::default();
        magic.clone_from_slice(&data[TAG_LENGTH..TAG_LENGTH + MAGIC_LENGTH]);

        if decoded_list.len() != 3 {
            debug!(
                "Failed to decode WHOAREYOU packet. Incorrect list size. Length: {}, expected 3",
                decoded_list.len()
            );
            return Err(PacketError::UnknownFormat);
        }

        let enr_seq_bytes = decoded_list.pop().expect("List is long enough");
        let mut enr_seq: [u8; 8] = Default::default();
        enr_seq.clone_from_slice(&enr_seq_bytes);
        let enr_seq = u64::from_be_bytes(enr_seq);

        let id_nonce_bytes = decoded_list.pop().expect("List is long enough");
        let mut id_nonce: [u8; ID_NONCE_LENGTH] = Default::default();
        id_nonce.clone_from_slice(&id_nonce_bytes);

        let token_bytes = decoded_list.pop().expect("List is long enough");
        let mut token: AuthTag = Default::default();
        token.clone_from_slice(&token_bytes);

        return Ok(Packet::WhoAreYou {
            tag,
            magic,
            token,
            id_nonce,
            enr_seq,
        });
    }

    /// Decodes a regular message into a `Packet`.
    fn decode_standard_message(tag: Tag, data: &[u8]) -> Result<Self, PacketError> {
        let rlp = rlp::Rlp::new(&data[TAG_LENGTH..TAG_LENGTH + AUTH_TAG_LENGTH + 1]);
        let auth_tag_bytes: Vec<u8> = match rlp.as_val() {
            Ok(v) => v,
            Err(_) => {
                debug!("Couldn't decode auth_tag for message: {:?}", data);
                return Err(PacketError::UnknownFormat);
            }
        };

        let mut auth_tag: AuthTag = Default::default();
        auth_tag.clone_from_slice(&auth_tag_bytes);

        return Ok(Packet::Message {
            tag,
            auth_tag,
            message: Box::new(data[TAG_LENGTH + AUTH_TAG_LENGTH + 1..].to_vec()),
        });
    }

    /// Decodes a message that contains an authentication header.
    fn decode_auth_header(tag: Tag, data: &[u8], rlp_length: usize) -> Result<Self, PacketError> {
        let auth_header_rlp = rlp::Rlp::new(&data[TAG_LENGTH..TAG_LENGTH + rlp_length]);
        let auth_header =
            AuthHeader::decode(&auth_header_rlp).map_err(|_| PacketError::UnknownFormat)?;

        let message_start = TAG_LENGTH + rlp_length;
        let message = Box::new(data[message_start..].to_vec());

        return Ok(Packet::AuthMessage {
            tag,
            auth_header,
            message,
        });
    }

    /// Decode raw bytes into a packet. The `magic` value (SHA2256(node-id, b"WHOAREYOU")) is passed as a parameter to check for
    /// the magic byte sequence.
    pub fn decode(data: &[u8], magic_data: &[u8]) -> Result<Self, PacketError> {
        // ensure the packet is large enough to contain the correct headers
        if data.len() < TAG_LENGTH + AUTH_TAG_LENGTH {
            debug!("Packet length too small. Length: {}", data.len());
            return Err(PacketError::TooSmall);
        }

        let mut tag: [u8; TAG_LENGTH] = Default::default();
        tag.clone_from_slice(&data[0..TAG_LENGTH]);

        // initially look for a WHOAREYOU packet
        if &data[TAG_LENGTH..TAG_LENGTH + MAGIC_LENGTH] == magic_data {
            return Packet::decode_whoareyou(tag, data);
        }
        // not a WHOAREYOU packet

        // check for RLP(bytes) or RLP(list)
        else if data[TAG_LENGTH] == 140 {
            // 8c in hex - rlp encoded bytes of length 12 -i.e rlp_bytes(auth_tag)
            // we have either a random-packet or standard message
            // return the encrypted standard message.
            return Packet::decode_standard_message(tag, data);
        }
        // not a Random Packet or standard message, may be a message with authentication header
        let rlp = rlp::Rlp::new(&data[TAG_LENGTH..]);
        if rlp.is_list() {
            // potentially authentication header

            let rlp_length = rlp
                .payload_info()
                .map_err(|_| {
                    debug!("Could not determine Auth header rlp length");
                    PacketError::UnknownFormat
                })?
                .total();

            return Packet::decode_auth_header(tag, data, rlp_length);
        }
        // the data is unrecognizable or corrupt.
        debug!("Failed identifying message: {:?}", data);
        Err(PacketError::UnknownPacket)
    }
}

#[derive(Debug, Clone)]
/// Types of packet errors.
pub enum PacketError {
    UnknownFormat,
    UnknownPacket,
    TooSmall,
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p_core::identity::Keypair;
    use rand;
    use sha2::{Digest, Sha256};
    //    use simple_logger;

    fn hash256_to_fixed_array(s: &'static str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.input(s);
        let mut result: [u8; 32] = std::default::Default::default();
        result.clone_from_slice(hasher.result().as_slice());
        result
    }

    #[test]
    fn encode_decode_random_packet() {
        //        let _ = simple_logger::init_with_level(log::Level::Debug);
        let tag = hash256_to_fixed_array("test-tag");
        let auth_tag: [u8; AUTH_TAG_LENGTH] = rand::random();
        let random_data: [u8; 44] = [17; 44];

        let packet = Packet::RandomPacket {
            tag: tag.clone(),
            auth_tag: auth_tag.clone(),
            data: Box::new(random_data.to_vec()),
        };

        let encoded_packet = packet.encode();
        let decoded_packet = Packet::decode(&encoded_packet, &random_data).unwrap();
        let expected_packet = Packet::Message {
            tag,
            auth_tag,
            message: Box::new(random_data.to_vec()),
        };

        assert_eq!(decoded_packet, expected_packet);
    }

    #[test]
    fn encode_decode_whoareyou_packet() {
        //        let _ = simple_logger::init_with_level(log::Level::Debug);
        let tag = hash256_to_fixed_array("test-tag");
        let magic = hash256_to_fixed_array("magic");
        let id_nonce: [u8; ID_NONCE_LENGTH] = rand::random();
        let token: [u8; AUTH_TAG_LENGTH] = rand::random();
        let enr_seq: u64 = rand::random();

        let packet = Packet::WhoAreYou {
            tag,
            magic,
            token,
            id_nonce,
            enr_seq,
        };

        let encoded_packet = packet.clone().encode();
        let decoded_packet = Packet::decode(&encoded_packet, &magic).unwrap();

        assert_eq!(decoded_packet, packet);
    }

    #[test]
    fn encode_decode_auth_packet() {
        //        let _ = simple_logger::init_with_level(log::Level::Debug);
        let tag = hash256_to_fixed_array("test-tag");

        // auth header data
        let auth_tag: [u8; AUTH_TAG_LENGTH] = rand::random();
        let ephemeral_pubkey = Keypair::generate_secp256k1()
            .public()
            .into_protobuf_encoding();
        let auth_response: [u8; 32] = rand::random();
        let auth_response = Box::new(auth_response.to_vec());

        let auth_header = AuthHeader {
            auth_tag,
            auth_scheme_name: "gsm",
            ephemeral_pubkey,
            auth_response,
        };

        let message: [u8; 16] = rand::random();
        let message = Box::new(message.to_vec());

        let packet = Packet::AuthMessage {
            tag,
            auth_header,
            message,
        };

        let encoded_packet = packet.clone().encode();
        let decoded_packet = Packet::decode(&encoded_packet, &tag).unwrap();

        assert_eq!(decoded_packet, packet);
    }

}
