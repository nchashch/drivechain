use bitcoin::blockdata::transaction::{OutPoint, Transaction};
use bitcoin::hash_types::{BlockHash};
use bitcoin::hashes::Hash;
use bitcoin::util::amount::Amount;
use bitcoin::util::psbt::serialize::{Deserialize, Serialize};
use serde::de::Error;

#[derive(Debug, Clone)]
pub struct MainDeposit {
    pub blockhash: BlockHash,
    pub ntx: usize,
    pub nburnindex: usize,
    pub tx: Transaction,
    pub nsidechain: usize,
    pub strdest: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerdeMainDeposit {
    blockhash: [u8; 32],
    ntx: usize,
    nburnindex: usize,
    tx: Vec<u8>,
    nsidechain: usize,
    strdest: String,
}

impl serde::Serialize for MainDeposit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let serialize_deposit = SerdeMainDeposit {
            blockhash: *self.blockhash.as_inner(),
            ntx: self.ntx,
            nburnindex: self.nburnindex,
            tx: self.tx.serialize(),
            nsidechain: self.nsidechain,
            strdest: self.strdest.clone(),
        };

        serialize_deposit.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for MainDeposit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match SerdeMainDeposit::deserialize(deserializer) {
            Ok(sd) => {
                let tx = match Transaction::deserialize(sd.tx.as_slice()) {
                    Ok(tx) => Ok(tx),
                    Err(err) => Err(D::Error::custom(err)),
                };
                let deposit = MainDeposit {
                    blockhash: BlockHash::from_inner(sd.blockhash),
                    ntx: sd.ntx,
                    nburnindex: sd.nburnindex,
                    tx: tx?,
                    nsidechain: sd.nsidechain,
                    strdest: sd.strdest,
                };
                Ok(deposit)
            }
            Err(err) => Err(D::Error::custom(err)),
        }
    }
}

impl MainDeposit {
    pub fn outpoint(&self) -> OutPoint {
        OutPoint {
            txid: self.tx.txid(),
            vout: self.nburnindex as u32,
        }
    }

    pub fn amount(&self) -> Amount {
        Amount::from_sat(self.tx.output[self.nburnindex].value)
    }

    pub fn is_spent_by(&self, other: &MainDeposit) -> bool {
        other
            .tx
            .input
            .iter()
            .filter(|input| input.previous_output == self.outpoint())
            .count()
            == 1
    }
}

/// A deposit that must be paid out in a sidechain block.
#[derive(Debug)]
pub struct Deposit {
    /// Sidechain address to which an `amount` of satoshi must be paid out.
    pub address: String,
    /// Amount of satoshi to be deposited.
    pub amount: u64,
}
