use std::str::FromStr;

use pallas_traverse::wellknown::GenesisValues;

lazy_static::lazy_static! {
    static ref MAINNET_GENESIS_VALUES: GenesisValues = GenesisValues::mainnet();
    static ref PREPROD_GENESIS_VALUES: GenesisValues = GenesisValues::preprod();
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
