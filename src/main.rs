mod lib;

use bitcoin::util::uint::{Uint256};
use bitcoin::hash_types::{BlockHash, Txid};
use bitcoin::util::amount::{Amount};
use rand::{thread_rng, Rng};
use std::collections::{HashMap};
use bitcoin::hashes::Hash;
use kv;

const THIS_SIDECHAIN: usize = 1;
const KEYHASH: &str = "0x175d41744409473f9381c3b32fe1de736bce6b2170a3495a76be01bc8778f775";

fn main() -> Result<(), lib::db::Error> {
    let user = "user".into();
    let password = "password".into();
    let drive = lib::Drivechain::new("./kvdb/", THIS_SIDECHAIN, KEYHASH.into(), user, password).unwrap();
    let str_dest = String::from("hhoojfiodsjiofsd");
    dbg!(drive.format_deposit_address(&str_dest));
    let sorted_deposits = drive.get_deposits()?;
    let mut deposits = HashMap::<Txid, lib::Deposit>::new();
    dbg!(sorted_deposits.iter().map(|dep| { (dep.blockhash, dep.tx.txid(), dep.nburnindex, dep.amount()) }).collect::<Vec<(BlockHash, Txid, usize, Amount)>>());
    deposits.extend(sorted_deposits.iter().map(|dep| { (dep.tx.txid(), dep.clone()) }));
    for txid in deposits.keys() {
        let deposit_amount = match deposits[txid].prev_txid {
            Some(prev_txid) => deposits[txid].amount() - deposits[&prev_txid].amount(),
            None => deposits[txid].amount(),
        };
        dbg!((txid, deposits[txid].prev_txid, deposit_amount));
    }
    let mut accounts = HashMap::<String, Amount>::new();
    for item in drive.db.deposits.iter() {
        let item = item?;
        let txid: kv::Raw = item.key()?;
        let txid = Txid::from_slice(txid.as_ref()).unwrap();
        let (address, amount) = drive.db.get_deposit_output(&txid)?.unwrap();
        dbg!((&txid, &address, &amount));
        if let Some(old_amount) = accounts.get_mut(&address) {
            *old_amount += amount;
        } else {
            accounts.insert(address, amount);
        }
    }
    dbg!(accounts);

    Ok(())
}

fn rand_bytes() -> Uint256 {
    let mut rng = thread_rng();
    let bytes: [u8; 32] = rng.gen();
    Uint256::from_be_bytes(bytes)
}
