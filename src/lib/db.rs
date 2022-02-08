use crate::lib::deposit::{Deposit};
use bitcoin::hashes::{Hash};
use bitcoin::hash_types::{Txid};
use bitcoin::util::amount::{Amount};
use kv::Codec;
use kv;
use std;

pub struct DB {
    pub store: kv::Store,
    pub deposits: kv::Bucket<'static, kv::Raw, kv::Bincode<Deposit>>,
    // pub unpaid_deposits: kv::Bucket<'static, kv::Raw, bool>,
    pub last_deposit: kv::Bucket<'static, String, kv::Raw>,
}

impl DB {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<DB, kv::Error> {
        let cfg = kv::Config::new(path);
        let store = kv::Store::new(cfg)?;
        let deposits = store.bucket::<kv::Raw, kv::Bincode<Deposit>>(Some("deposits"))?;
        // let unpaid_deposits = store.bucket::<kv::Raw, bool>(Some("unpaid_deposits"))?;
        let last_deposit = store.bucket::<String, kv::Raw>(Some("last_deposit"))?;
        Ok(DB{
            store: store,
            deposits: deposits,
            last_deposit: last_deposit,
        })
    }

    pub fn update(&self, deposits: &[Deposit]) -> Result<(), kv::Error> {
        let mut batch = kv::Batch::<kv::Raw, kv::Bincode<Deposit>>::new();
        for deposit in deposits {
            let txid = deposit.tx.txid();
            batch.set(txid.as_inner(), kv::Bincode(deposit.clone()))?;
        }
        self.deposits.batch(batch)?;
        Ok(())
    }

    pub fn get_last_deposit(&self) -> Result<Option<(Txid, usize)>, Error> {
        let txid = match self.last_deposit.get("last_deposit")? {
            Some(txid) => txid,
            None => return Ok(None),
        };
        let txid = Txid::from_slice(txid.as_ref()).unwrap();
        let last_deposit = self.get_deposit(&txid)?.map(|deposit| {
            (txid, deposit.nburnindex)
        });
        Ok(last_deposit)
    }

    pub fn get_deposit(&self, txid: &Txid) -> Result<Option<Deposit>, Error> {
        let deposit = self.deposits.get(txid.as_inner())?.map(|deposit| {
            deposit.into_inner()
        });
        Ok(deposit)
    }

    pub fn get_deposit_output(&self, txid: &Txid) -> Result<Option<(String, Amount)>, Error> {
        // We try to get the deposit for this txid.
        let deposit = match self.deposits.get(txid.as_inner())? {
            Some(deposit) => deposit.into_inner(),
            // If we don't find the deposit - we return Ok(None). This is
            // different from an error.
            //
            // Ok(None) means deposit wasn't found but DB is working properly.
            // Err(err) means that there was a DB error or that DB is in an
            // inconsistent state.
            None => return Ok(None),
        };
        // We see if it refers to a previous deposit transaction.
        let prev_txid = match deposit.prev_txid {
            Some(prev_txid) => prev_txid,
            None => {
                // If it doesn't have a previous deposit it means that it is the
                // first deposit in the sidechain.
                //
                // So output amount = current deposit CTIP amount.
                let output = (deposit.strdest.clone(), deposit.amount());
                return Ok(Some(output));
            },
        };
        // If it does we try to get the previous deposit from database.
        let prev_deposit = match self.deposits.get(prev_txid.as_inner())? {
            Some(prev_deposit) => prev_deposit.into_inner(),
            // If we fail that means that the database is in an inconsistent
            // state, so we return an error.
            None => return Err(Error::PrevDepositNotFound),
        };
        // If prev deposit exists then the output amount is the difference
        // between current and previous CTIP amount.
        let output = (deposit.strdest.clone(), deposit.amount() - prev_deposit.amount());
        Ok(Some(output))
    }
}

#[derive(Debug)]
pub enum Error {
    KVError(kv::Error),
    PrevDepositNotFound,
    LastDepositNotFound,
}

impl From<kv::Error> for Error {
    fn from(err: kv::Error) -> Error {
        Error::KVError(err)
    }
}
