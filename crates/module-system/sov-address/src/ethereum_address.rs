#![allow(dead_code)]

#[cfg(feature = "native")]
use std::str::FromStr;

use alloy_primitives::AddressError;
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(feature = "native")]
use private_key::EthereumPrivateKey;
use reth_primitives::keccak256;
use reth_primitives::revm_primitives::Address;
use schemars::JsonSchema;
use secp256k1::constants::PUBLIC_KEY_SIZE;
use secp256k1::ecdsa::Signature;
use secp256k1::{Message, PublicKey, SECP256K1};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use sov_modules_api::macros::UniversalWallet;
use sov_modules_api::{BasicAddress, CryptoSpec, HexString};
use sov_rollup_interface::crypto::{CredentialId, PublicKeyHex, SigVerificationError};

use crate::{MultiAddress, Not28Bytes};

pub type MultiAddressEvm = MultiAddress<EthereumAddress>;

impl BasicAddress for EthereumAddress {}
impl Not28Bytes for EthereumAddress {}

#[cfg(test)]
mod evm_spec_address_tests {
    use std::str::FromStr;

    use borsh::{BorshDeserialize, BorshSerialize};
    use sov_modules_api::configurable_spec::ConfigurableSpec;
    use sov_modules_api::execution_mode::Native;
    use sov_modules_api::Spec;
    use sov_test_utils::{MockDaSpec, MockZkvm};

    use super::*;
    type S = ConfigurableSpec<MockDaSpec, MockZkvm, MockZkvm, MultiAddressEvm, Native>;

    #[test]
    fn test_serde_json_multi_address_evm_vm() {
        let address = MultiAddressEvm::Vm(
            EthereumAddress::from_str("0x71334bf1710D12c9f689cC819476fA589F08C64C").unwrap(),
        );
        let serialized = serde_json::to_string(&address).unwrap();
        let deserialized: MultiAddressEvm = serde_json::from_str(&serialized).unwrap();
        assert_eq!(address, deserialized);
    }

    #[test]
    fn test_serde_json_multi_address_evm_standard() {
        let address = MultiAddressEvm::Standard(
            sov_modules_api::Address::from_str(
                "sov1pv9skzctpv9skzctpv9skzctpv9skzctpv9skzctpv9skqm7ehv",
            )
            .unwrap(),
        );
        let serialized = serde_json::to_string(&address).unwrap();
        let deserialized: MultiAddressEvm = serde_json::from_str(&serialized).unwrap();
        assert_eq!(address, deserialized);
    }

    #[test]
    fn test_bincode_multi_address_evm_vm() {
        let address = MultiAddressEvm::Vm(
            EthereumAddress::from_str("0x71334bf1710D12c9f689cC819476fA589F08C64C").unwrap(),
        );
        let serialized = bincode::serialize(&address).unwrap();
        let deserialized: MultiAddressEvm = bincode::deserialize(&serialized).unwrap();
        assert_eq!(address, deserialized);
    }

    #[test]
    fn test_bincode_multi_address_evm_standard() {
        let address = MultiAddressEvm::Standard(
            sov_modules_api::Address::from_str(
                "sov1pv9skzctpv9skzctpv9skzctpv9skzctpv9skzctpv9skqm7ehv",
            )
            .unwrap(),
        );
        let serialized = bincode::serialize(&address).unwrap();
        let deserialized: MultiAddressEvm = bincode::deserialize(&serialized).unwrap();
        assert_eq!(address, deserialized);
    }

    #[test]
    fn test_borsh_allowed_sequencer() {
        #[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
        pub struct TypeWithFieldAfterAddress<S: Spec> {
            address: S::Address,
            variable: u64,
        }

        let allowed_sequencer = TypeWithFieldAfterAddress {
            address: MultiAddressEvm::Vm(
                EthereumAddress::from_str("0x71334bf1710D12c9f689cC819476fA589F08C64C").unwrap(),
            ),
            variable: 90000000000,
        };
        let mut serialized: Vec<u8> = Vec::new();
        BorshSerialize::serialize(&allowed_sequencer, &mut serialized).unwrap();
        let deserialized: TypeWithFieldAfterAddress<S> =
            BorshDeserialize::try_from_slice(&serialized).unwrap();
        assert_eq!(allowed_sequencer, deserialized);
    }

    #[test]
    fn test_borsh_multi_address_evm_vm() {
        let spec_address = MultiAddressEvm::Vm(
            EthereumAddress::from_str("0x71334bf1710D12c9f689cC819476fA589F08C64C").unwrap(),
        );
        let mut spec_address_bytes: Vec<u8> = Vec::new();
        BorshSerialize::serialize(&spec_address, &mut spec_address_bytes).unwrap();
        let deserialized: MultiAddressEvm =
            BorshDeserialize::try_from_slice(spec_address_bytes.as_slice()).unwrap();
        assert_eq!(spec_address, deserialized);
    }

    #[test]
    fn test_borsh_multi_address_evm_standard() {
        let standard_address = MultiAddressEvm::Standard(
            sov_modules_api::Address::from_str(
                "sov1pv9skzctpv9skzctpv9skzctpv9skzctpv9skzctpv9skqm7ehv",
            )
            .unwrap(),
        );
        let mut spec_address_bytes: Vec<u8> = Vec::new();
        BorshSerialize::serialize(&standard_address, &mut spec_address_bytes).unwrap();
        let deserialized: MultiAddressEvm =
            BorshDeserialize::try_from_slice(spec_address_bytes.as_slice()).unwrap();
        assert_eq!(standard_address, deserialized);
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    UniversalWallet,
)]
#[cfg_attr(
    feature = "arbitrary",
    derive(
        sov_modules_api::prelude::arbitrary::Arbitrary,
        sov_modules_api::prelude::proptest_derive::Arbitrary
    )
)]
/// A standard 20 byte Ethereum address with checksum.
pub struct EthereumAddress(#[sov_wallet(as_ty = "[u8;20]", display = "hex")] pub Address);

impl From<CredentialId> for EthereumAddress {
    fn from(credential_id: CredentialId) -> Self {
        credential_id.0.into()
    }
}

impl From<HexString<[u8; 32]>> for EthereumAddress {
    fn from(value: HexString<[u8; 32]>) -> Self {
        Self::try_from(&value.0.as_slice()[12..32]).unwrap()
    }
}

impl<'a> From<&'a EthereumPublicKey> for EthereumAddress {
    fn from(value: &'a EthereumPublicKey) -> Self {
        // Construct the address from the 64 bytes of public key material (2x32 byte field elements), stripping
        // out the `UNCOMPRESSED` tag which is the first byte. https://github.com/bitcoin-core/secp256k1/blob/8deef00b33ca81202aca80fe0bcd9730f084fbd2/src/eckey_impl.h#L49
        Self(Address::from_raw_public_key(
            &value.pub_key.serialize_uncompressed()[1..],
        ))
    }
}

impl BorshSerialize for EthereumAddress {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.0 .0 .0)
    }
}

impl AsRef<[u8]> for EthereumAddress {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl TryFrom<&[u8]> for EthereumAddress {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(Address::try_from(value)?))
    }
}

impl std::str::FromStr for EthereumAddress {
    type Err = AddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Address::parse_checksummed(s, None)?))
    }
}

impl BorshDeserialize for EthereumAddress {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut bytes = [0u8; 20];
        reader.read_exact(&mut bytes)?;
        Ok(Self(bytes.into()))
    }
}

impl From<Address> for EthereumAddress {
    fn from(value: Address) -> Self {
        Self(value)
    }
}

impl From<EthereumAddress> for [u8; 20] {
    fn from(value: EthereumAddress) -> Self {
        value.0.into()
    }
}

impl From<EthereumAddress> for Address {
    fn from(value: EthereumAddress) -> Self {
        value.0
    }
}

impl std::fmt::Display for EthereumAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl schemars::JsonSchema for EthereumAddress {
    fn schema_name() -> String {
        "EthereumAddress".to_string()
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::from_value(serde_json::json!({
            "type": "string",
            "pattern": "^0x[a-fA-F0-9]{64}$",
            "description": "20 bytes in hexadecimal format, with `0x` prefix.",
        }))
        .unwrap()
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
    BorshDeserialize,
    BorshSerialize,
)]
/// A [`CryptoSpec`] implementation for EVM rollups. Uses the secp256 signature scheme with
/// keccak256 hashes for signatures, and sha256 as the default hasher for other operations.
pub struct EvmCryptoSpec;

impl CryptoSpec for EvmCryptoSpec {
    #[cfg(feature = "native")]
    type PrivateKey = EthereumPrivateKey;

    type PublicKey = EthereumPublicKey;

    type Hasher = Sha256;

    type Signature = EthereumSignature;
}

/// Defines private key types and operations
#[cfg(feature = "native")]
pub mod private_key {

    use rand::rngs::OsRng;
    use reth_primitives::keccak256;
    use secp256k1::{Keypair, Message};
    #[cfg(feature = "arbitrary")]
    use sov_rollup_interface::crypto::PrivateKey;

    use super::{EthereumPublicKey, EthereumSignature};

    /// A private key for the sepc256k1 signature scheme.
    /// This struct also stores the corresponding public key.
    #[derive(Clone, serde::Serialize, serde::Deserialize)]
    #[serde(transparent)]
    pub struct EthereumPrivateKey {
        key_pair: secp256k1::Keypair,
    }

    impl core::fmt::Debug for EthereumPrivateKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("EthereumPrivateKey")
                .field("public_key", &self.key_pair.public_key())
                .field("private_key", &"***REDACTED***")
                .finish()
        }
    }

    impl sov_rollup_interface::crypto::PrivateKey for EthereumPrivateKey {
        type PublicKey = EthereumPublicKey;

        type Signature = EthereumSignature;

        fn generate() -> Self {
            let mut csprng = OsRng;

            Self {
                key_pair: Keypair::new_global(&mut csprng),
            }
        }

        fn pub_key(&self) -> Self::PublicKey {
            EthereumPublicKey {
                pub_key: self.key_pair.public_key(),
            }
        }

        fn sign(&self, msg: &[u8]) -> Self::Signature {
            let digest = Message::from_digest(keccak256(msg).into());
            EthereumSignature {
                msg_sig: self.key_pair.secret_key().sign_ecdsa(digest),
            }
        }
    }

    impl EthereumPrivateKey {
        /// Returns the private key as a hex string.
        pub fn as_hex(&self) -> String {
            hex::encode(self.key_pair.secret_bytes())
        }
    }

    #[cfg(feature = "arbitrary")]
    mod arbitrary_impls {
        use proptest::prelude::{any, BoxedStrategy};
        use proptest::strategy::Strategy;
        use rand::rngs::StdRng;
        use rand::SeedableRng;

        use super::*;

        impl<'a> arbitrary::Arbitrary<'a> for EthereumPrivateKey {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                // it is important to generate the secret deterministically from the arbitrary argument
                // so keys and signatures will be reproducible for a given seed.
                // this unlocks fuzzy replay
                let seed = <[u8; 32]>::arbitrary(u)?;
                let rng = &mut StdRng::from_seed(seed);
                let key_pair = Keypair::new_global(rng);

                Ok(Self { key_pair })
            }
        }

        impl<'a> arbitrary::Arbitrary<'a> for EthereumPublicKey {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                EthereumPrivateKey::arbitrary(u).map(|p| p.pub_key())
            }
        }

        impl<'a> arbitrary::Arbitrary<'a> for EthereumSignature {
            fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
                // the secret/public pair is lost; it is impossible to verify this signature
                // to run a verification, generate the keys+payload individually
                let payload_len = u.arbitrary_len::<u8>()?;
                let payload = u.bytes(payload_len)?;
                EthereumPrivateKey::arbitrary(u).map(|s| s.sign(payload))
            }
        }

        impl proptest::arbitrary::Arbitrary for EthereumPrivateKey {
            type Parameters = ();
            type Strategy = BoxedStrategy<Self>;

            fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
                any::<[u8; 32]>()
                    .prop_map(|seed| Self {
                        key_pair: Keypair::new_global(&mut StdRng::from_seed(seed)),
                    })
                    .boxed()
            }
        }

        impl proptest::arbitrary::Arbitrary for EthereumPublicKey {
            type Parameters = ();
            type Strategy = BoxedStrategy<Self>;

            fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
                any::<EthereumPrivateKey>()
                    .prop_map(|key| key.pub_key())
                    .boxed()
            }
        }

        impl proptest::arbitrary::Arbitrary for EthereumSignature {
            type Parameters = ();
            type Strategy = BoxedStrategy<Self>;

            fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
                any::<(EthereumPrivateKey, Vec<u8>)>()
                    .prop_map(|(key, bytes)| key.sign(&bytes))
                    .boxed()
            }
        }
    }
}

/// The public key of an secp256k1 keypair. Wraps the optimized Risc0 fork of the ed25519-dalek crate.
#[derive(PartialEq, Eq, Hash, Clone, Debug, JsonSchema, UniversalWallet)]
pub struct EthereumPublicKey {
    #[schemars(
        flatten,
        with = "String",
        length(equal = "secp256k1::constants::PUBLIC_KEY_SIZE * 2")
    )]
    #[sov_wallet(as_ty = "[u8; secp256k1::constants::PUBLIC_KEY_SIZE]")]
    pub(crate) pub_key: PublicKey,
}

impl EthereumPublicKey {
    /// Returns the bytes of the underlying public key.
    pub fn bytes(&self) -> [u8; PUBLIC_KEY_SIZE] {
        self.pub_key.serialize()
    }
}

impl sov_rollup_interface::crypto::PublicKey for EthereumPublicKey {
    fn credential_id(&self) -> sov_rollup_interface::crypto::CredentialId {
        // Use the same method as EthereumAddress::from(&EthereumPublicKey)
        // Get the uncompressed public key bytes (without the prefix byte)
        let uncompressed = self.pub_key.serialize_uncompressed();
        let hash: [u8; 32] = keccak256(&uncompressed[1..]).into();

        sov_rollup_interface::crypto::CredentialId(hash.into())
    }
}

impl BorshDeserialize for EthereumPublicKey {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buffer = [0; PUBLIC_KEY_SIZE];
        reader.read_exact(&mut buffer)?;

        let pub_key = PublicKey::from_slice(&buffer).map_err(map_error)?;

        Ok(Self { pub_key })
    }
}

impl BorshSerialize for EthereumPublicKey {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.pub_key.serialize())
    }
}

/// A secp256k1 signature. Wraps the rust-secp256k1 crate.
#[derive(
    PartialEq, Eq, Debug, Clone, serde::Serialize, serde::Deserialize, JsonSchema, UniversalWallet,
)]
pub struct EthereumSignature {
    /// The inner signature.
    #[schemars(flatten, with = "String", length(equal = "128"))]
    #[sov_wallet(as_ty = "[u8; 64]")]
    pub msg_sig: Signature,
}

impl BorshDeserialize for EthereumSignature {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let mut buffer = [0; 64];
        reader.read_exact(&mut buffer)?;

        Ok(Self {
            msg_sig: Signature::from_compact(&buffer).map_err(map_error)?,
        })
    }
}

impl BorshSerialize for EthereumSignature {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_all(&self.msg_sig.serialize_compact())
    }
}

impl TryFrom<&[u8]> for EthereumSignature {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self {
            msg_sig: Signature::from_compact(value).map_err(anyhow::Error::msg)?,
        })
    }
}

impl sov_rollup_interface::crypto::Signature for EthereumSignature {
    type PublicKey = EthereumPublicKey;

    fn verify(&self, pub_key: &Self::PublicKey, msg: &[u8]) -> Result<(), SigVerificationError> {
        let digest = Message::from_digest(keccak256(msg).into());
        pub_key
            .pub_key
            .verify(SECP256K1, &digest, &self.msg_sig)
            .map_err(|e| SigVerificationError {
                error: e.to_string(),
            })
    }
}

fn map_error(e: secp256k1::Error) -> std::io::Error {
    std::io::Error::other(e)
}

#[cfg(feature = "native")]
impl FromStr for EthereumPublicKey {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let pk_hex = PublicKeyHex::try_from(s)?;
        EthereumPublicKey::try_from(&pk_hex)
    }
}

#[cfg(feature = "native")]
impl FromStr for EthereumSignature {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(s)?;

        let signature: Signature =
            Signature::from_compact(&bytes).map_err(|_| anyhow::anyhow!("Invalid signature"))?;

        Ok(EthereumSignature { msg_sig: signature })
    }
}

impl serde::Serialize for EthereumPublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serde::Serialize::serialize(&PublicKeyHex::from(self), serializer)
        } else {
            serde::Serialize::serialize(&self.pub_key, serializer)
        }
    }
}

impl<'de> serde::Deserialize<'de> for EthereumPublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let pub_key_hex: PublicKeyHex = serde::Deserialize::deserialize(deserializer)?;
            Ok(EthereumPublicKey::try_from(&pub_key_hex).map_err(serde::de::Error::custom)?)
        } else {
            let pub_key = serde::Deserialize::deserialize(deserializer)?;
            Ok(EthereumPublicKey { pub_key })
        }
    }
}

#[cfg(test)]
mod tests {
    use sov_rollup_interface::crypto::PrivateKey;

    use super::*;

    #[test]
    fn test_privatekey_serde_bincode() {
        let key_pair = EthereumPrivateKey::generate();
        let serialized = bincode::serialize(&key_pair).expect("Serialization to vec is infallible");
        let output = bincode::deserialize::<EthereumPrivateKey>(&serialized)
            .expect("SigningKey is serialized correctly");

        assert_eq!(key_pair.as_hex(), output.as_hex());
    }

    #[test]
    fn test_privatekey_serde_json() {
        use self::private_key::EthereumPrivateKey;

        let key_pair = EthereumPrivateKey::generate();
        let serialized = serde_json::to_vec(&key_pair).expect("Serialization to vec is infallible");
        let output = serde_json::from_slice::<EthereumPrivateKey>(&serialized)
            .expect("Keypair is serialized correctly");

        assert_eq!(key_pair.as_hex(), output.as_hex());
    }
}

#[cfg(test)]
#[cfg(all(feature = "arbitrary", feature = "native"))]
mod proptest_tests {
    use proptest::collection::vec;
    use proptest::prelude::*;
    use sov_modules_api::{PrivateKey, Signature};

    use super::*;

    proptest! {
        #[test]
        fn pub_key_json_schema_is_valid(item in any::<EthereumPublicKey>()) {
            let serialized = serde_json::to_value(item).unwrap();
            let schema = serde_json::to_value(schemars::schema_for!(EthereumPublicKey)).unwrap();

            jsonschema::validate(&schema, &serialized).unwrap();
        }

        #[test]
        fn sig_json_schema_is_valid(item in any::<EthereumSignature>()) {
            let serialized = serde_json::to_value(item).unwrap();
            let schema = serde_json::to_value(schemars::schema_for!(EthereumSignature)).unwrap();

            jsonschema::validate(&schema, &serialized).unwrap();
        }

        #[test]
        fn sig_verification_works(msg in vec(any::<u8>(), 0..100)) {
            let key = EthereumPrivateKey::generate();
            let signature = key.sign(&msg);
            let pubkey = key.pub_key();
            assert!(signature.verify(&pubkey, &msg).is_ok());
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pub_key_json() {
        let pub_key_hex: PublicKeyHex =
            "0204e690e67bd9d8cfc9c310ad3468de11416feefcc86da6e73613b89677a61b80"
                .try_into()
                .unwrap();

        let pub_key = EthereumPublicKey::try_from(&pub_key_hex).unwrap();
        let pub_key_str: String = serde_json::to_string(&pub_key).unwrap();

        assert_eq!(
            pub_key_str,
            r#""0204e690e67bd9d8cfc9c310ad3468de11416feefcc86da6e73613b89677a61b80""#
        );

        let deserialized: EthereumPublicKey = serde_json::from_str(&pub_key_str).unwrap();
        assert_eq!(deserialized, pub_key);
    }
}

impl From<&EthereumPublicKey> for PublicKeyHex {
    fn from(pub_key: &EthereumPublicKey) -> Self {
        let hex = hex::encode(pub_key.bytes());
        // UNWRAP: conversion to SafeString can error in only two cases: non-printable-ascii and too long.
        // A hex::encoded string should always be printable ascii, and a public key is 33 bytes =
        // 66 hex characters, well below the 128 character SafeString limit.
        Self {
            hex: hex.try_into().unwrap(),
        }
    }
}

impl TryFrom<&PublicKeyHex> for EthereumPublicKey {
    type Error = anyhow::Error;

    fn try_from(pub_key: &PublicKeyHex) -> Result<Self, Self::Error> {
        let bytes = hex::decode(&pub_key.hex)?;

        let bytes: [u8; PUBLIC_KEY_SIZE] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid public key size"))?;

        let pub_key =
            PublicKey::from_slice(&bytes).map_err(|_| anyhow::anyhow!("Invalid public key"))?;

        Ok(Self { pub_key })
    }
}

#[cfg(test)]
mod hex_tests {
    use sov_rollup_interface::crypto::{PrivateKey, PublicKey};

    use super::*;

    #[test]
    fn test_pub_key_hex() {
        let pub_key = EthereumPrivateKey::generate().pub_key();
        let pub_key_hex = PublicKeyHex::from(&pub_key);
        let converted_pub_key = EthereumPublicKey::try_from(&pub_key_hex).unwrap();
        assert_eq!(pub_key, converted_pub_key);
    }

    #[test]
    fn test_pub_key_hex_str() {
        let key = "0204e690e67bd9d8cfc9c310ad3468de11416feefcc86da6e73613b89677a61b80";
        let pub_key_hex_lower: PublicKeyHex = key.try_into().unwrap();
        let pub_key_hex_upper: PublicKeyHex = key.to_uppercase().try_into().unwrap();

        let pub_key_lower = EthereumPublicKey::try_from(&pub_key_hex_lower).unwrap();
        let pub_key_upper = EthereumPublicKey::try_from(&pub_key_hex_upper).unwrap();

        assert_eq!(pub_key_lower, pub_key_upper);
    }

    #[test]
    fn test_key_to_addr() {
        let decoded = hex::decode("226634643938363630643866613337303631353535653933333134303737393434646131336530333964393237616433643366303762396136396430336339366122").unwrap();
        let key: EthereumPrivateKey = serde_json::from_slice(&decoded).unwrap();
        let found_addr: EthereumAddress = (&(key.pub_key())).into();
        assert_eq!(
            found_addr,
            EthereumAddress::from_str("0x71334bf1710D12c9f689cC819476fA589F08C64C").unwrap()
        );
    }

    #[test]
    fn test_ethereum_public_key_credential_id_to_address() {
        // Create a known private key
        let decoded = hex::decode("226634643938363630643866613337303631353535653933333134303737393434646131336530333964393237616433643366303762396136396430336339366122").unwrap();
        let private_key: EthereumPrivateKey = serde_json::from_slice(&decoded).unwrap();

        // Get the public key
        let public_key = private_key.pub_key();

        // Get the ethereum address directly from public key
        let eth_address: EthereumAddress = (&public_key).into();

        // Get the ethereum address from public key turned into credential id turned into address
        let credential_id = public_key.credential_id();
        let address_from_credential_id: EthereumAddress = credential_id.into();

        // They should be equal
        assert_eq!(eth_address, address_from_credential_id);

        // Verify against known address
        let expected_address =
            EthereumAddress::from_str("0x71334bf1710D12c9f689cC819476fA589F08C64C").unwrap();
        assert_eq!(eth_address, expected_address);
    }
}
