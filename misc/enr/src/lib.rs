//! # Ethereum Node Record (ENR)
//!
//! This crate contains an implementation of an Ethereum Node Record (ENR) as specified by [EIP-778](https://eips.ethereum.org/EIPS/eip-778) extended to allow for the use of a range of public key types.
//!
//! An ENR is a signed, key-value record which as an associated `NodeId` (a 32-byte identifier).
//! Updating/modifying an ENR requires a libp2p [`Keypair`] in order to re-sign the record with
//! the associated key-pair. This implementation builds uses [`EnrKeypair`] as a wrapper around
//! the libp2p [`Keypair`] in order to perform ENR-specific sign/verify functions.
//!
//! ENR's are identified by their sequence number. When updating an ENR, the sequence number is
//! increased.
//!
//! This implementation also keeps the `rlp_encoding` of it's content, to ensure the ordering of the
//! keys when encoded/decoded.
//!
//! # Example
//!
//! To build an ENR, an [`EnrBuilder`] is provided.
//!
//! Example (Building an ENR):
//!
//! ```rust
//! use enr::EnrBuilder;
//! use libp2p_core::identity::Keypair;
//! use std::net::Ipv4Addr;
//!
//! let key = Keypair::generate_secp256k1();
//! let ip = Ipv4Addr::new(192,168,0,1);
//! let enr = EnrBuilder::new().ip(ip.into()).tcp(8000).id("v5").build(&key).unwrap();
//!
//! assert_eq!(enr.multiaddr()[0],
//!     "/ip4/192.168.0.1/tcp/8000".parse().unwrap());
//! assert_eq!(enr.ip(), Some("192.168.0.1".parse().unwrap()));
//! assert_eq!(enr.id(), Some(String::from("v5")));
//! ```
//!
//! [`Keypair`]: libp2p_core::identity::Keypair
//! [`EnrKeypair`]: crate::enr_keypair::EnrKeypair
//! [`Enr`]: crate::enr::Enr
//! [`EnrBuilder`]: crate::enr::EnrBuilder
//! [`NodeId`]: crate::enr::NodeId

mod enr_keypair;

use crate::enr_keypair::{EnrKeypair, EnrPublicKey};
use base64;
use bs58;
use libp2p_core::identity::{ed25519, Keypair, PublicKey};
use log::debug;
use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use libp2p_core::{
    identity::{rsa, secp256k1 as libp2p_secp256k1},
    multiaddr::{Multiaddr, Protocol},
    PeerId,
};

const MAX_ENR_SIZE: usize = 300;

/// The identifier for an ENR record. This is the keccak256 hash of the public key (for secp256k1
/// this is the uncompressed encoded form of the public key).
pub type NodeId = [u8; 32];

/// The ENR Record.
///
/// This struct will always have a valid signature, known public key type, sequence number and `NodeId`. All other parameters are variable/optional.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Enr {
    /// ENR sequence number.
    pub seq: u64,

    /// The `NodeId` of the ENR record.
    pub node_id: NodeId,

    /// Key-value contents of the ENR.
    content: HashMap<String, Vec<u8>>,

    /// RLP-encoded content. This exists because Hashmaps do not preserve ordering and the signature
    /// is order-dependant. This is updated every time the ENR is updated.
    rlp_content: Vec<u8>,

    /// The signature of the ENR record, stored as bytes.
    signature: Vec<u8>,
}

impl Enr {
    // getters //

    /// The libp2p PeerId for the record.
    pub fn peer_id(&self) -> PeerId {
        self.public_key().into()
    }

    /// Returns a list of multiaddrs if the ENR has an `ip` and either a `tcp` or `udp` key. The
    /// vector remains empty if these fields are not defined.
    pub fn multiaddr(&self) -> Vec<Multiaddr> {
        let mut multiaddrs: Vec<Multiaddr> = Vec::new();
        if let Some(ip) = self.ip() {
            if let Some(udp) = self.udp() {
                let mut multiaddr: Multiaddr = ip.into();
                multiaddr.push(Protocol::Udp(udp));
                multiaddrs.push(multiaddr);
            }

            if let Some(tcp) = self.tcp() {
                let mut multiaddr: Multiaddr = ip.into();
                multiaddr.push(Protocol::Tcp(tcp));
                multiaddrs.push(multiaddr);
            }
        }
        multiaddrs
    }

    /// Returns the IP address of the ENR record if it is defined.
    pub fn ip(&self) -> Option<IpAddr> {
        if let Some(ip_bytes) = self.content.get("ip") {
            return match ip_bytes.len() {
                4 => {
                    let mut ip = [0u8; 4];
                    ip.copy_from_slice(ip_bytes);
                    Some(IpAddr::from(ip))
                }
                16 => {
                    let mut ip = [0u8; 16];
                    ip.copy_from_slice(ip_bytes);
                    Some(IpAddr::from(ip))
                }
                _ => None,
            };
        }
        None
    }

    /// Returns the Id of ENR record if it is defined.
    pub fn id(&self) -> Option<String> {
        if let Some(id_bytes) = self.content.get("id") {
            return Some(String::from_utf8_lossy(id_bytes).to_string());
        }
        None
    }

    /// Returns the TCP port of ENR record if it is defined.
    pub fn tcp(&self) -> Option<u16> {
        if let Some(tcp_bytes) = self.content.get("tcp") {
            if tcp_bytes.len() <= 2 {
                let mut tcp = [0u8; 2];
                tcp[2 - tcp_bytes.len()..].copy_from_slice(tcp_bytes);
                return Some(u16::from_be_bytes(tcp));
            }
        }
        None
    }

    /// Returns the UDP port of ENR record if it is defined.
    pub fn udp(&self) -> Option<u16> {
        if let Some(udp_bytes) = self.content.get("udp") {
            if udp_bytes.len() <= 2 {
                let mut udp = [0u8; 2];
                udp[2 - udp_bytes.len()..].copy_from_slice(udp_bytes);
                return Some(u16::from_be_bytes(udp));
            }
        }
        None
    }

    /// Returns a socket (based on the UDP port), if the IP and UDP fields are specified.
    // This is primarily used for discv5 for which this library was built.
    pub fn udp_socket(&self) -> Option<SocketAddr> {
        if let Some(ip) = self.ip() {
            if let Some(udp) = self.udp() {
                return Some(SocketAddr::new(ip, udp));
            }
        }
        None
    }

    /// Returns the signature of the ENR record.
    pub fn signature(&self) -> &[u8] {
        &self.signature
    }

    /// Returns the public key of the ENR record.
    ///
    /// Currently supported public keys are `secp256k1`, `ed25519`, and `rsa`.
    pub fn public_key(&self) -> PublicKey {
        // Must have a known public key type.
        // TODO: Build a mapping of known pubkeys
        if let Some(pubkey_bytes) = self.content.get("secp256k1") {
            return libp2p_secp256k1::PublicKey::decode(pubkey_bytes)
                .map(PublicKey::Secp256k1)
                .expect("Valid secp256k1 key");
        } else if let Some(pubkey_bytes) = self.content.get("ed25519") {
            return ed25519::PublicKey::decode(pubkey_bytes)
                .map(PublicKey::Ed25519)
                .expect("Valid ed25519 public key");
        } else if let Some(pubkey_bytes) = self.content.get("rsa") {
            return rsa::PublicKey::decode_x509(pubkey_bytes)
                .map(PublicKey::Rsa)
                .expect("Valid rsa public key");
        }
        panic!("An ENR was created with an unknown public key");
    }

    /// Verify the signature of the ENR record.
    pub fn verify(&self) -> bool {
        let enr_pubkey = EnrPublicKey::from(self.public_key());
        return enr_pubkey.verify(&self.rlp_content, &self.signature);
    }

    /// RLP encodes the ENR into a byte array.
    pub fn encode(self) -> Vec<u8> {
        let mut s = RlpStream::new();
        s.append(&self);
        s.drain()
    }

    /// Provides the URL-safe base64 encoded "text" version of the ENR.
    pub fn to_base64(&self) -> String {
        let cloned_self = self.clone();
        base64::encode_config(&cloned_self.encode(), base64::URL_SAFE)
    }

    /// Returns the current size of the ENR.
    pub fn size(&self) -> usize {
        self.rlp_content.len()
    }

    // Setters //

    /// Adds a key/value to the ENR record. A `Keypair` is required to re-sign the record once
    /// modified.
    pub fn add_key(
        &mut self,
        key: &str,
        value: Vec<u8>,
        keypair: Keypair,
    ) -> Result<bool, EnrError> {
        self.content.insert(key.into(), value);
        // add the new public key
        // convert the libp2p keypair into an EnrKeypair
        let enr_keypair = EnrKeypair::from(keypair.clone());
        let public_key = enr_keypair.public();
        self.content
            .insert(public_key.clone().into(), public_key.encode());
        // increment the sequence number
        self.seq += 1;

        // construct compact signature
        let signature = enr_keypair
            .sign(&self.rlp_content())
            .map_err(|_| EnrError::SigningError)?;

        // update the node id
        self.node_id = Enr::node_id(&keypair.public());

        // check the size of the record
        if self.rlp_content.len() + signature.len() + 8 > MAX_ENR_SIZE {
            return Err(EnrError::ExceedsMaxSize);
        }

        Ok(true)
    }

    pub fn set_ip(&mut self, ip: IpAddr, keypair: Keypair) -> Result<bool, EnrError> {
        let ip_bytes = match ip {
            IpAddr::V4(addr) => addr.octets().to_vec(),
            IpAddr::V6(addr) => addr.octets().to_vec(),
        };
        self.add_key("ip", ip_bytes, keypair)
    }

    pub fn set_udp(&mut self, udp: u16, keypair: Keypair) -> Result<bool, EnrError> {
        self.add_key("udp", udp.to_be_bytes().to_vec(), keypair)
    }

    pub fn set_tcp(&mut self, tcp: u16, keypair: Keypair) -> Result<bool, EnrError> {
        self.add_key("tcp", tcp.to_be_bytes().to_vec(), keypair)
    }

    pub fn set_public_key(&mut self, keypair: &Keypair) {
        let enr_public = EnrKeypair::from(keypair.clone()).public();
        self.content
            .insert(enr_public.clone().into(), enr_public.encode());
    }

    // Private Functions //

    /// Evaluates the RLP-encoding of the content of the ENR record.
    fn rlp_content(&self) -> Vec<u8> {
        let mut stream = RlpStream::new();
        stream.begin_list(self.content.len() * 2 + 1);
        stream.append(&self.seq);
        for (k, v) in self.content.iter() {
            stream.append(k);
            stream.append(v);
        }
        stream.drain()
    }

    /// Returns the node-id of the associated ENR record. This is the keccak256
    /// hash of the public key. ENR records cannot be created without a valid public key.
    /// Therefore this will always return a value.
    fn node_id(public_key: &PublicKey) -> NodeId {
        let pubkey_bytes = EnrPublicKey::from(public_key.clone()).encode_uncompressed();
        let mut node_id: NodeId = [0; 32];
        let hash = Keccak256::digest(&pubkey_bytes);
        node_id.copy_from_slice(&hash);
        node_id
    }
}

impl std::fmt::Display for Enr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ENR")
            .field("NodeId", &bs58::encode(self.node_id).into_string())
            //.field("PeerId", self.peer_id().to_base(58))
            .field("seq", &self.seq)
            .field("ip", &self.ip())
            .field("tcp", &self.tcp())
            .field("udp", &self.udp())
            .field("public key", &self.public_key())
            .finish()
    }
}

/// This is the builder struct for generating ENR records.
pub struct EnrBuilder {
    /// The starting sequence number for the ENR record.
    seq: u64,

    /// The key-value pairs for the ENR record.
    content: HashMap<String, Vec<u8>>,
}

impl EnrBuilder {
    /// Constructs a minimal `EnrBuilder` providing only a sequence number.
    pub fn new() -> Self {
        EnrBuilder {
            seq: 1,
            content: HashMap::new(),
        }
    }

    /// Modifies the sequence number of the builder.
    pub fn seq(&mut self, seq: u64) -> &mut Self {
        self.seq = seq;
        self
    }

    /// Adds an arbitrary key-value to the `ENRBuilder`.
    pub fn add_value(&mut self, key: String, value: Vec<u8>) -> &mut Self {
        self.content.insert(key, value);
        self
    }

    /// Adds an `ip` field to the `ENRBuilder`.
    pub fn ip(&mut self, ip: IpAddr) -> &mut Self {
        let key = String::from("ip");
        match ip {
            IpAddr::V4(addr) => {
                self.content.insert(key, addr.octets().to_vec());
            }
            IpAddr::V6(addr) => {
                self.content.insert(key, addr.octets().to_vec());
            }
        }
        self
    }

    /// Adds an `Id` field to the `ENRBuilder`.
    pub fn id(&mut self, id: &str) -> &mut Self {
        self.content.insert("id".into(), id.as_bytes().to_vec());
        self
    }

    /// Adds a `tcp` field to the `ENRBuilder`.
    pub fn tcp(&mut self, tcp: u16) -> &mut Self {
        self.content
            .insert("tcp".into(), tcp.to_be_bytes().to_vec());
        self
    }

    /// Adds a `udp` field to the `ENRBuilder`.
    pub fn udp(&mut self, udp: u16) -> &mut Self {
        self.content
            .insert("udp".into(), udp.to_be_bytes().to_vec());
        self
    }

    /// Generates the rlp-encoded form of the ENR specified by the builder config.
    fn rlp_content(&self) -> Vec<u8> {
        let mut stream = RlpStream::new();
        stream.begin_list(self.content.len() * 2 + 1);
        stream.append(&self.seq);
        for (k, v) in self.content.iter() {
            stream.append(k);
            stream.append(v);
        }
        stream.drain()
    }

    /// Adds a public key to the ENR builder.
    fn add_public_key(&mut self, key: &EnrPublicKey) {
        self.add_value(key.clone().into(), key.encode());
    }

    /// Constructs an ENR from the ENRBuilder struct.
    pub fn build(&mut self, key: &Keypair) -> Result<Enr, EnrError> {
        let enr_key = EnrKeypair::from(key.clone());
        self.add_public_key(&enr_key.public());
        let rlp_content = self.rlp_content();

        // construct compact signature
        let signature = enr_key
            .sign(&rlp_content)
            .map_err(|_| EnrError::SigningError)?;

        // check the size of the record
        if rlp_content.len() + signature.len() + 8 > MAX_ENR_SIZE {
            return Err(EnrError::ExceedsMaxSize);
        }

        Ok(Enr {
            seq: self.seq,
            node_id: Enr::node_id(&key.public()),
            content: self.content.clone(),
            rlp_content,
            signature,
        })
    }
}

impl Encodable for Enr {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(self.content.len() * 2 + 2);
        s.append(&self.signature);
        s.append(&self.seq);
        for (k, v) in self.content.iter() {
            s.append(k);
            s.append(v);
        }
    }
}

impl Decodable for Enr {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        if !rlp.is_list() {
            debug!("Failed to decode ENR. Not an RLP list: {}", rlp);
            return Err(DecoderError::RlpExpectedToBeList);
        }

        let mut decoded_list = rlp.as_list::<Vec<u8>>().map_err(|_| {
            debug!("Could not decode content: {}", rlp);
            DecoderError::Custom("List decode fail")
        })?;

        if decoded_list.len() % 2 != 0 {
            debug!("Failed to decode ENR. List size is not a multiple of 2.");
            return Err(DecoderError::Custom("List not a multiple of two"));
        }

        let signature = decoded_list.remove(0);
        let seq_bytes = decoded_list.remove(0);

        if seq_bytes.len() > 8 {
            debug!("Failed to decode ENR. Sequence number is not a u64.");
            return Err(DecoderError::Custom("Invalid Sequence number"));
        }

        // build u64 from big endian vec<u8>
        let mut seq: [u8; 8] = [0; 8];
        seq[8 - seq_bytes.len()..].copy_from_slice(&seq_bytes);
        let seq = u64::from_be_bytes(seq);

        // keep track of the current rlp ordering
        let mut rlp_encodings: Vec<Vec<u8>> = Vec::new();

        let mut content = HashMap::new();
        for _ in 0..decoded_list.len() / 2 {
            let value = decoded_list.pop().expect("Large enough");
            let key = decoded_list.pop().expect("Large enough");

            // keep current ordering in reverse
            rlp_encodings.push(value.clone());
            rlp_encodings.push(key.clone());

            let key = String::from_utf8_lossy(&key);
            content.insert(key.to_string(), value);
        }

        rlp_encodings.push(seq_bytes);
        let rev_rlp_encodings: Vec<Vec<u8>> = rlp_encodings.iter().cloned().rev().collect();

        let rlp_content = rlp::encode_list::<Vec<u8>, Vec<u8>>(&rev_rlp_encodings);

        // verify we know the signature type
        let public_key = {
            if let Some(pubkey_bytes) = content.get("secp256k1") {
                libp2p_secp256k1::PublicKey::decode(pubkey_bytes)
                    .map(PublicKey::Secp256k1)
                    .map_err(|_| DecoderError::Custom("Invalid Secp256k1 Signature"))?
            } else if let Some(pubkey_bytes) = content.get("ed25519") {
                ed25519::PublicKey::decode(pubkey_bytes)
                    .map(PublicKey::Ed25519)
                    .map_err(|_| DecoderError::Custom("Invalid ed25519 Signature"))?
            } else if let Some(pubkey_bytes) = content.get("rsa") {
                rsa::PublicKey::decode_x509(pubkey_bytes)
                    .map(PublicKey::Rsa)
                    .map_err(|_| DecoderError::Custom("Invalid rsa Signature"))?
            } else {
                return Err(DecoderError::Custom("Unknown signature"));
            }
        };

        // calculate the node id
        let node_id = Enr::node_id(&public_key);

        let enr = Enr {
            seq,
            node_id,
            signature,
            content,
            rlp_content,
        };

        // verify the signature before returning
        // if the public key is of an unknown type, this will fail.
        // An ENR record will always have a valid public-key and therefore node-id
        if !enr.verify() {
            return Err(DecoderError::Custom("Invalid Signature"));
        }
        Ok(enr)
    }
}

#[derive(Clone, Debug)]
pub enum EnrError {
    ExceedsMaxSize,
    SigningError,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn check_test_vector() {
        let valid_record = hex::decode("f884b8407098ad865b00a582051940cb9cf36836572411a47278783077011599ed5cd16b76f2635f4e234738f30813a89eb9137e3e3df5266e3a1f11df72ecf1145ccb9c01826964827634826970847f00000189736563703235366b31a103ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd31388375647082765f").unwrap();
        let signature = hex::decode("7098ad865b00a582051940cb9cf36836572411a47278783077011599ed5cd16b76f2635f4e234738f30813a89eb9137e3e3df5266e3a1f11df72ecf1145ccb9c").unwrap();
        let expected_pubkey =
            hex::decode("03ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138")
                .unwrap();

        let enr = rlp::decode::<Enr>(&valid_record).unwrap();

        let pubkey = match enr.public_key() {
            PublicKey::Secp256k1(key) => Some(key.encode()),
            _ => None,
        };

        assert_eq!(enr.ip(), Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))));
        assert_eq!(enr.id(), Some(String::from("v4")));
        assert_eq!(enr.udp(), Some(30303));
        assert_eq!(enr.tcp(), None);
        assert_eq!(enr.signature(), &signature[..]);
        assert_eq!(pubkey.unwrap().to_vec(), expected_pubkey);
    }

    #[test]
    fn test_encode_decode_secp256k1() {
        let key = Keypair::generate_secp256k1();

        let id = "v5";
        let ip = Ipv4Addr::new(127, 0, 0, 1);
        let tcp = 3000;

        let enr = {
            let mut builder = EnrBuilder::new();
            builder.id(id);
            builder.ip(ip.into());
            builder.tcp(tcp);
            builder.build(&key).unwrap()
        };

        let encoded_enr = rlp::encode(&enr);

        let decoded_enr = rlp::decode::<Enr>(&encoded_enr).unwrap();

        assert_eq!(decoded_enr.id(), Some(id.into()));
        assert_eq!(decoded_enr.ip(), Some(ip.into()));
        assert_eq!(decoded_enr.tcp(), Some(tcp));
        // Must compare encoding as the public key itself can be different
        assert_eq!(
            decoded_enr.public_key().into_protobuf_encoding(),
            key.public().into_protobuf_encoding()
        );
    }

    #[test]
    fn test_encode_decode_ed25519() {
        let key = Keypair::generate_ed25519();

        let id = "v5";
        let ip = Ipv4Addr::new(10, 0, 0, 1);
        let tcp = 30303;

        let enr = {
            let mut builder = EnrBuilder::new();
            builder.id(id);
            builder.ip(ip.into());
            builder.tcp(tcp);
            builder.build(&key).unwrap()
        };

        let encoded_enr = rlp::encode(&enr);
        let decoded_enr = rlp::decode::<Enr>(&encoded_enr).unwrap();

        assert_eq!(decoded_enr.id(), Some(id.into()));
        assert_eq!(decoded_enr.ip(), Some(ip.into()));
        assert_eq!(decoded_enr.tcp(), Some(tcp));
        assert_eq!(decoded_enr.public_key(), key.public());
    }

    #[test]
    fn test_add_key() {
        let key = Keypair::generate_secp256k1();
        let id = "v5";
        let ip = Ipv4Addr::new(10, 0, 0, 1);
        let tcp = 30303;

        let mut enr = {
            let mut builder = EnrBuilder::new();
            builder.id(id);
            builder.ip(ip.into());
            builder.tcp(tcp);
            builder.build(&key).unwrap()
        };

        assert!(enr.add_key("random", Vec::new(), key).unwrap());
    }

    #[test]
    fn test_set_ip() {
        let key = Keypair::generate_secp256k1();
        let id = "v5";
        let tcp = 30303;
        let ip = Ipv4Addr::new(10, 0, 0, 1);

        let mut enr = {
            let mut builder = EnrBuilder::new();
            builder.id(id);
            builder.tcp(tcp);
            builder.build(&key).unwrap()
        };

        assert!(enr.set_ip(ip.into(), key.clone()).unwrap());
        assert_eq!(enr.id(), Some(id.into()));
        assert_eq!(enr.ip(), Some(ip.into()));
        assert_eq!(enr.tcp(), Some(tcp));

        // Compare the encoding as the key itself can be differnet
        assert_eq!(
            enr.public_key().into_protobuf_encoding(),
            key.public().into_protobuf_encoding()
        );
    }

    #[test]
    fn test_multiaddr() {
        let key = Keypair::generate_secp256k1();
        let tcp = 30303;
        let udp = 30304;
        let ip = Ipv4Addr::new(10, 0, 0, 1);

        let enr = {
            let mut builder = EnrBuilder::new();
            builder.ip(ip.into());
            builder.tcp(tcp);
            builder.udp(udp);
            builder.build(&key).unwrap()
        };

        assert_eq!(
            enr.multiaddr()[0],
            "/ip4/10.0.0.1/udp/30304".parse().unwrap()
        );
        assert_eq!(
            enr.multiaddr()[1],
            "/ip4/10.0.0.1/tcp/30303".parse().unwrap()
        );
    }
}
