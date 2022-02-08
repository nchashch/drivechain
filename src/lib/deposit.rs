use bitcoin::util::amount::{Amount, Denomination};
use serde::de::{Deserializer, Error as SerdeError};
use bitcoin::util::psbt::serialize::{Serialize, Deserialize};
use bitcoin::blockdata::transaction::{Transaction, OutPoint, TxOut};
use bitcoin::hash_types::{Txid, BlockHash};
use bitcoin::hashes::{Hash};

#[derive(Debug, Clone)]
pub struct Deposit {
    pub blockhash: BlockHash,
    pub ntx: usize,
    pub nburnindex: usize,
    pub tx: Transaction,
    pub nsidechain: usize,
    pub strdest: String,
    pub prev_txid: Option<Txid>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerdeDeposit {
    blockhash: [u8;32],
    ntx: usize,
    nburnindex: usize,
    tx: Vec<u8>,
    nsidechain: usize,
    strdest: String,
    prev_txid: Option<[u8;32]>,
}

impl serde::Serialize for Deposit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let serialize_deposit = SerdeDeposit{
            blockhash: self.blockhash.as_inner().clone(),
            ntx: self.ntx,
            nburnindex: self.nburnindex,
            tx: self.tx.serialize(),
            nsidechain: self.nsidechain,
            strdest: self.strdest.clone(),
            prev_txid: self.prev_txid.map(|txid| { txid.as_inner().clone() }),
        };

        serialize_deposit.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Deposit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match SerdeDeposit::deserialize(deserializer) {
            Ok(sd) => {
                let tx = match Transaction::deserialize(sd.tx.as_slice()) {
                    Ok(tx) => Ok(tx),
                    Err(err) => Err(D::Error::custom(err)),
                };
                let deposit = Deposit{
                    blockhash: BlockHash::from_inner(sd.blockhash),
                    ntx: sd.ntx,
                    nburnindex: sd.nburnindex,
                    tx: tx?,
                    nsidechain: sd.nsidechain,
                    strdest: sd.strdest,
                    prev_txid: sd.prev_txid.map(|txid| { Txid::from_inner(txid) }),
                };
                Ok(deposit)
            },
            Err(err) => Err(D::Error::custom(err)),
        }
    }
}

impl Deposit {
    pub fn outpoint(&self) -> OutPoint {
        OutPoint {
            txid: self.tx.txid(),
            vout: self.nburnindex as u32,
        }
    }

    pub fn output(&self) -> TxOut {
        self.tx.output[self.nburnindex].clone()
    }

    pub fn amount(&self) -> Amount {
        Amount::from_sat(self.tx.output[self.nburnindex].value)
    }
}
