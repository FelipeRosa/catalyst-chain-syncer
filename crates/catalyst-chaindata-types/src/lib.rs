pub mod serde_size;

use std::str::FromStr;

use chrono::{DateTime, Utc};
use cryptoxide::digest::Digest as _;
use minicbor::Decode;
use pallas_primitives::conway::Metadatum;
use pallas_traverse::{wellknown::GenesisValues, MultiEraBlock, MultiEraMeta, MultiEraTx};
use tracing::warn;

lazy_static::lazy_static! {
    static ref MAINNET_GENESIS_VALUES: GenesisValues = GenesisValues::mainnet();
    static ref PREPROD_GENESIS_VALUES: GenesisValues = GenesisValues::preprod();
}

const CATALYST_REGISTRATION_METADATA_KEY: u64 = 61284;
const CATALYST_REGISTRATION_WITNESS_KEY: u64 = 61285;

pub enum CatalystRegistrationVotingKey {
    Legacy([u8; cryptoxide::ed25519::PUBLIC_KEY_LENGTH]),
    Delegations(Vec<([u8; cryptoxide::ed25519::PUBLIC_KEY_LENGTH], u32)>),
}

impl<'b, C> Decode<'b, C> for CatalystRegistrationVotingKey {
    fn decode(
        d: &mut minicbor::Decoder<'b>,
        _ctx: &mut C,
    ) -> Result<Self, minicbor::decode::Error> {
        let ty = d.datatype()?;

        match ty {
            minicbor::data::Type::Bytes => {
                let voting_key: minicbor::bytes::ByteArray<32> = d
                    .decode()
                    .map_err(|e| e.with_message("Decoding CIP-15 voting key"))?;

                Ok(Self::Legacy(*voting_key))
            }
            minicbor::data::Type::Array => {
                let Some(delegations_count) = d.array()? else {
                    return Err(minicbor::decode::Error::message(
                        "Delegations array must have a known size",
                    ));
                };

                let mut delegations = Vec::new();

                for _ in 0..delegations_count {
                    let Some(2) = d.array()? else {
                        return Err(minicbor::decode::Error::message(
                            "Delegation entry array must have size = 2",
                        ));
                    };

                    let voting_key: minicbor::bytes::ByteArray<32> = d
                        .decode()
                        .map_err(|e| e.with_message("Decoding CIP-36 delegation voting key"))?;

                    let weight = d
                        .u32()
                        .map_err(|e| e.with_message("Decoding CIP-36 delegation weight"))?;

                    delegations.push((*voting_key, weight));
                }

                Ok(Self::Delegations(delegations))
            }
            _ => Err(minicbor::decode::Error::message(
                "Expected voting key or delegations array",
            )),
        }
    }
}

// Catalyst registration schema:
// https://github.com/cardano-foundation/CIPs/blob/master/CIP-0036/schema.cddl
//
// Legacy:
// https://github.com/cardano-foundation/CIPs/blob/master/CIP-0015/schema.cddl
#[derive(Decode)]
#[cbor(map)]
pub struct CatalystRegistration {
    #[cbor(skip)]
    pub transaction_hash: [u8; 32],
    #[cbor(skip)]
    pub stake_credential: [u8; 28],
    #[n(1)]
    pub voting_key: CatalystRegistrationVotingKey,
    #[n(2)]
    pub stake_public_key: minicbor::bytes::ByteArray<32>,
    #[n(3)]
    pub payment_address: minicbor::bytes::ByteVec,
    #[n(4)]
    pub nonce: u64,
    #[n(5)]
    pub voting_purpose: Option<u32>,
}

#[derive(Decode)]
#[cbor(map)]
struct CatalystRegistrationWitness {
    #[b(1)]
    bytes: minicbor::bytes::ByteArray<64>,
}

impl CatalystRegistrationWitness {
    fn parse_metadatum(metadatum: &Metadatum) -> anyhow::Result<Self> {
        if let Metadatum::Map(m) = metadatum {
            let wit: CatalystRegistrationWitness =
                minicbor::decode(&pallas_codec::minicbor::to_vec(m)?)
                    .map_err(|e| anyhow::anyhow!(e))?;

            Ok(wit)
        } else {
            anyhow::bail!("Registration witness metadatum must be a map");
        }
    }
}

struct CatalystRegistrationMetadatumWithKey<'a>(&'a Metadatum);

impl<'b, C> pallas_codec::minicbor::Encode<C> for CatalystRegistrationMetadatumWithKey<'b> {
    fn encode<W: pallas_codec::minicbor::encode::Write>(
        &self,
        e: &mut pallas_codec::minicbor::Encoder<W>,
        _ctx: &mut C,
    ) -> Result<(), pallas_codec::minicbor::encode::Error<W::Error>> {
        e.map(1)?;
        e.u32(61284)?;
        e.encode(self.0)?;

        Ok(())
    }
}

impl CatalystRegistration {
    pub fn from_transaction(tx: &MultiEraTx, network: Network) -> anyhow::Result<Option<Self>> {
        // TODO: Refactor this
        let (reg, raw_reg, wit) = match tx.metadata() {
            MultiEraMeta::Empty => return Ok(None),
            MultiEraMeta::NotApplicable => return Ok(None),
            MultiEraMeta::AlonzoCompatible(pairs) => {
                let mut opt_reg = None;
                let mut opt_wit = None;

                for (k, v) in pairs.iter() {
                    match *k {
                        CATALYST_REGISTRATION_METADATA_KEY => {
                            let raw = pallas_codec::minicbor::to_vec(
                                CatalystRegistrationMetadatumWithKey(v),
                            )
                            .map_err(|e| anyhow::anyhow!(e))?;

                            opt_reg = Some((Self::parse_metadatum(tx, v)?, raw));
                        }
                        CATALYST_REGISTRATION_WITNESS_KEY => {
                            opt_wit = Some(CatalystRegistrationWitness::parse_metadatum(v)?);
                        }
                        _ => {}
                    }
                }

                if let (Some((reg, raw_reg)), Some(wit)) = (opt_reg, opt_wit) {
                    (reg, raw_reg, wit)
                } else {
                    return Ok(None);
                }
            }
            _ => anyhow::bail!("Unknown transaction metadata kind"),
        };

        if !reg.valid_payment_address(network) {
            anyhow::bail!("Invalid Catalyst registration payment address");
        }

        let mut reg_hash = [0u8; 32];
        {
            let mut hash_ctx = cryptoxide::blake2b::Blake2b::new(32);
            hash_ctx.input(&raw_reg);
            hash_ctx.result(&mut reg_hash);
        }

        if !cryptoxide::ed25519::verify(&reg_hash, &reg.stake_public_key, &wit.bytes) {
            anyhow::bail!("Catalyst registration signature verification failed");
        }

        Ok(Some(reg))
    }

    fn parse_metadatum(tx: &MultiEraTx, metadatum: &Metadatum) -> anyhow::Result<Self> {
        if let Metadatum::Map(m) = metadatum {
            let mut reg: CatalystRegistration =
                minicbor::decode(&pallas_codec::minicbor::to_vec(m)?)
                    .map_err(|e| anyhow::anyhow!(e))?;

            reg.transaction_hash = *tx.hash();

            // Stake credential
            {
                let mut blake2b = cryptoxide::blake2b::Blake2b::new(28);
                blake2b.input(reg.stake_public_key.as_slice());
                blake2b.result(&mut reg.stake_credential);
            }

            Ok(reg)
        } else {
            anyhow::bail!("Registration metadatum must be a map");
        }
    }

    fn valid_payment_address(&self, network: Network) -> bool {
        let Some(address_prefix_byte) = self.payment_address.first() else {
            return false;
        };

        let address_type = address_prefix_byte >> 4 & 0xf;
        let address_network = address_prefix_byte & 0xf;

        if let Network::Mainnet = network {
            if address_network != 1 {
                return false;
            }
        }

        let valid_addrs = [0, 1, 2, 3, 4, 5, 6, 7, 14, 15];

        valid_addrs.contains(&address_type)
    }
}

pub struct CardanoBlock {
    pub block_no: u64,
    pub slot_no: u64,
    pub epoch_no: u64,
    pub network: Network,
    pub block_time: DateTime<Utc>,
    pub block_hash: [u8; 32],
    pub previous_hash: Option<[u8; 32]>,
}

impl CardanoBlock {
    pub fn from_block(block: &MultiEraBlock, network: Network) -> anyhow::Result<Self> {
        Ok(Self {
            block_no: block.number(),
            slot_no: block.slot(),
            epoch_no: block.epoch(network.genesis_values()).0,
            network,
            block_time: DateTime::from_timestamp(
                block.wallclock(network.genesis_values()) as i64,
                0,
            )
            .ok_or_else(|| anyhow::anyhow!("Failed to parse DateTime from timestamp"))?,
            block_hash: *block.hash(),
            previous_hash: block.header().previous_hash().as_ref().map(|h| **h),
        })
    }
}

pub struct CardanoTransaction {
    pub hash: [u8; 32],
    pub block_no: u64,
    pub network: Network,
}

impl CardanoTransaction {
    pub fn many_from_block(block: &MultiEraBlock, network: Network) -> anyhow::Result<Vec<Self>> {
        let data = block
            .txs()
            .into_iter()
            .map(|tx| Self {
                hash: *tx.hash(),
                block_no: block.number(),
                network,
            })
            .collect();

        Ok(data)
    }
}

pub struct CardanoTxo {
    pub transaction_hash: [u8; 32],
    pub index: u32,
    pub value: u64,
    pub stake_credential: Option<[u8; 28]>,
}

impl CardanoTxo {
    pub fn from_transaction(tx: &MultiEraTx) -> Vec<Self> {
        let data = tx
            .outputs()
            .into_iter()
            .zip(0..)
            .filter_map(|(tx_output, index)| {
                let address = match tx_output.address() {
                    Ok(addr) => addr,
                    Err(e) => {
                        warn!(error = ?e, "Failed to parse TXO");
                        return None;
                    }
                };

                let stake_credential = match address {
                    pallas_addresses::Address::Byron(_) => None,
                    pallas_addresses::Address::Shelley(address) => address.try_into().ok(),
                    pallas_addresses::Address::Stake(stake_address) => Some(stake_address),
                };

                Some(Self {
                    transaction_hash: *tx.hash(),
                    index,
                    value: tx_output.lovelace_amount(),
                    stake_credential: stake_credential.map(|a| **a.payload().as_hash()),
                })
            })
            .collect::<Vec<_>>();

        data
    }
}

pub struct CardanoSpentTxo {
    pub from_transaction_hash: [u8; 32],
    pub index: u32,
    pub to_transaction_hash: [u8; 32],
}

impl CardanoSpentTxo {
    pub fn from_transaction(tx: &MultiEraTx) -> Vec<Self> {
        let data = tx
            .inputs()
            .into_iter()
            .map(|tx_input| Self {
                from_transaction_hash: **tx_input.output_ref().hash(),
                index: tx_input.output_ref().index() as u32,
                to_transaction_hash: *tx.hash(),
            })
            .collect();

        data
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Network {
    Mainnet,
    Preprod,
}

impl Network {
    pub fn id(&self) -> u16 {
        match self {
            Network::Mainnet => 0,
            Network::Preprod => 1,
        }
    }

    pub fn genesis_values(&self) -> &'static GenesisValues {
        match self {
            Network::Mainnet => &MAINNET_GENESIS_VALUES,
            Network::Preprod => &PREPROD_GENESIS_VALUES,
        }
    }
}

impl FromStr for Network {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mainnet" => Ok(Self::Mainnet),
            "preprod" => Ok(Self::Preprod),
            _ => Err(anyhow::format_err!("Unknown network: '{}'", s)),
        }
    }
}
