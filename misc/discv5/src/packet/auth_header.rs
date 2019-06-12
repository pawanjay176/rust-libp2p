///! The Authentication header associated with Discv5 Packets.
use super::{AuthTag, AUTH_TAG_LENGTH};
use enr::Enr;
// use libp2p_core::identity::PublicKey;
use log::debug;
use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};

//TODO: generalise the scheme and associated crypto library.
const AUTH_SCHEME_NAME: &str = "gsm";

/// The Authentication header.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthHeader {
    /// Authentication nonce.
    pub auth_tag: AuthTag,

    /// The authentication scheme.
    pub auth_scheme_name: &'static str,

    /// The public key as a raw encoded bytes.
    pub ephemeral_pubkey: Vec<u8>,

    /// Authentication response.
    pub auth_response: Box<Vec<u8>>,
}

impl AuthHeader {
    pub fn new(auth_tag: AuthTag, ephemeral_pubkey: Vec<u8>, resp: Box<Vec<u8>>) -> Self {
        AuthHeader {
            auth_tag,
            auth_scheme_name: AUTH_SCHEME_NAME,
            ephemeral_pubkey,
            auth_response: resp,
        }
    }

    /// RLP-encodes the authentication header.
    pub fn encode(&self) -> Vec<u8> {
        let mut s = RlpStream::new();
        s.append(self);
        s.drain()
    }
}

/// An authentication response. This contains a signed challenge nonce, and optionally an updated
/// ENR if the requester has an outdated ENR.
pub struct AuthResponse {
    /// A signature of the challenge nonce.
    pub signature: Vec<u8>,

    /// An optional ENR, required if the requester has an out-dated ENR.
    pub updated_enr: Option<Enr>,
}

impl AuthResponse {
    pub fn new(sig: &[u8], updated_enr: Option<Enr>) -> Self {
        AuthResponse {
            signature: sig.to_vec(),
            updated_enr,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut s = RlpStream::new();
        s.append(self);
        s.drain()
    }
}

impl Encodable for AuthResponse {
    fn rlp_append(&self, s: &mut RlpStream) {
        if let Some(enr) = &self.updated_enr {
            s.begin_list(2);
            s.append(&self.signature.to_vec());
            s.append(&rlp::encode(&enr.clone()));
        } else {
            s.append(&self.signature.to_vec());
        }
    }
}

impl Decodable for AuthResponse {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        if !rlp.is_list() {
            let signature = rlp.decoder().decode_value(|bytes| Ok(bytes.to_vec()))?;
            return Ok(AuthResponse {
                signature,
                updated_enr: None,
            });
        } else {
            let mut list = rlp.as_list::<Vec<u8>>().map_err(|e| {
                dbg!(e);
                DecoderError::Custom("List decode fail. Error: {:?}")
            })?;

            if list.len() != 2 {
                debug!("Failed to decode Authentication response. Incorrect list size. Length: {}, expected 2", list.len());
                return Err(DecoderError::RlpExpectedToBeList);
            }

            let enr_bytes = list.pop().expect("value exists");
            let updated_enr = Some(rlp::decode::<Enr>(&enr_bytes)?);
            let signature = list.pop().expect("value exists");

            Ok(AuthResponse {
                signature: signature.to_vec(),
                updated_enr,
            })
        }
    }
}

impl Encodable for AuthHeader {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(4);
        s.append(&self.auth_tag.to_vec());
        s.append(&self.auth_scheme_name);
        s.append(&self.ephemeral_pubkey.clone());
        s.append(&self.auth_response.to_vec());
    }
}

impl Decodable for AuthHeader {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        if !rlp.is_list() {
            debug!(
                "Failed to decode Authentication header. Not an RLP list: {}",
                rlp
            );
            return Err(DecoderError::RlpExpectedToBeList);
        }

        let mut decoded_list = match rlp.as_list::<Vec<u8>>() {
            Ok(v) => v,
            Err(_) => {
                debug!("Could not decode Authentication header: {}", rlp);
                return Err(DecoderError::Custom("List decode fail"));
            }
        };

        if decoded_list.len() != 4 {
            debug!("Failed to decode Authentication header. Incorrect list size. Length: {}, expected 4", decoded_list.len());
            return Err(DecoderError::RlpExpectedToBeList);
        }

        let auth_response = decoded_list.pop().expect("List is long enough");
        let pubkey_bytes = decoded_list.pop().expect("List is long enough");
        let auth_scheme_bytes = decoded_list.pop().expect("List is long enough");
        let auth_tag_bytes = decoded_list.pop().expect("List is long enough");

        let mut auth_tag: AuthTag = Default::default();
        if auth_tag_bytes.len() != AUTH_TAG_LENGTH {
            return Err(DecoderError::Custom("Invalid Authtag length"));
        }
        auth_tag.clone_from_slice(&auth_tag_bytes);

        // currently only support gsm scheme
        if String::from_utf8_lossy(&auth_scheme_bytes) != "gsm" {
            debug!(
                "Failed to decode Authentication header. Unknown auth scheme: {}",
                String::from_utf8_lossy(&auth_scheme_bytes)
            );
            return Err(DecoderError::Custom("Invalid Authentication Scheme"));
        }

        /* Not decoding into libp2p public keys - this is done later
        let ephemeral_pubkey = PublicKey::from_protobuf_encoding(&pubkey_bytes).map_err(|_| {
            debug!(
                "Failed to decode Authentication header. Unknown publickey encoding: {:?}",
                pubkey_bytes
            );
            DecoderError::Custom("Unknown public key encoding")
        })?;
        */
        let ephemeral_pubkey = pubkey_bytes.clone();

        Ok(AuthHeader {
            auth_tag,
            auth_scheme_name: "gsm",
            ephemeral_pubkey,
            auth_response: Box::new(auth_response),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enr::EnrBuilder;
    use libp2p_core::identity::Keypair;
    use rand;

    #[test]
    fn encode_decode_auth_response() {
        let sig: [u8; 32] = rand::random();

        let key = Keypair::generate_secp256k1();
        let id = "v5";
        let tcp = 30303;

        let enr = {
            let mut builder = EnrBuilder::new();
            builder.id(id);
            builder.tcp(tcp);
            builder.build(&key).unwrap()
        };

        let auth_response = AuthResponse::new(&sig, Some(enr.clone()));

        let encoded_auth_response = auth_response.encode();

        let decoded_auth_response = rlp::decode::<AuthResponse>(&encoded_auth_response).unwrap();

        assert_eq!(decoded_auth_response.signature, sig);
        assert_eq!(decoded_auth_response.updated_enr, Some(enr));
    }

    #[test]
    fn encode_decode_auth_response_no_enr() {
        let sig: [u8; 32] = rand::random();

        let auth_response = AuthResponse::new(&sig, None);

        let encoded_auth_response = auth_response.encode();

        let decoded_auth_response = rlp::decode::<AuthResponse>(&encoded_auth_response).unwrap();

        assert_eq!(decoded_auth_response.signature, sig);
    }

}
