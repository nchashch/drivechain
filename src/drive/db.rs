use super::deposit::Deposit;
use super::withdrawal;
use super::withdrawal::WithdrawalOutput;
use bincode;
use bitcoin::blockdata::transaction::OutPoint;
use bitcoin::hashes::Hash;
use byteorder::{BigEndian, ByteOrder};
use sled;
use sled::Transactional;
use std;
use std::collections::HashMap;

const DEPOSITS: &[u8] = b"deposits";
const WITHDRAWALS: &[u8] = b"withdrawals";
const WITHDRAWAL_STATUSES: &[u8] = b"withdrawal_statuses";
const UNSPENT_WITHDRAWALS: &[u8] = b"unspent_withdrawals";
const BLOCK_NUMBER_TO_WTIDS: &[u8] = b"block_number_to_wtids";
pub struct DB {
    pub db: sled::Db,
    pub deposits: sled::Tree,
    pub withdrawals: sled::Tree,
    pub unspent_withdrawals: sled::Tree,
    pub withdrawal_statuses: sled::Tree,
    // block_number => [wtid]
    pub block_number_to_wtids: sled::Tree,
}

// deposit
// bundle 1-1..* withdrawal

impl DB {
    // Here panicking is appropriate becuase failing to open sidechain db is not
    // a recoverable error.
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> DB {
        let db = sled::open(path).expect("couldn't open sled db");
        let deposits = db
            .open_tree(DEPOSITS)
            .expect("couldn't open deposits key value store");
        let withdrawals = db
            .open_tree(WITHDRAWALS)
            .expect("couldn't open withdrawals key value store");
        let withdrawal_statuses = db
            .open_tree(WITHDRAWAL_STATUSES)
            .expect("couldn't open withdrawal statuses key value store");
        let unspent_withdrawals = db
            .open_tree(UNSPENT_WITHDRAWALS)
            .expect("couldn't open unspent withdrawals key value store");
        let block_number_to_wtids = db
            .open_tree(BLOCK_NUMBER_TO_WTIDS)
            .expect("couldn't open block number to wtids key value store");
        DB {
            db: db,
            deposits: deposits,
            withdrawals: withdrawals,
            withdrawal_statuses: withdrawal_statuses,
            unspent_withdrawals: unspent_withdrawals,
            block_number_to_wtids: block_number_to_wtids,
        }
    }

    pub fn set_bundle_withdrawal_status(
        &mut self,
        bundle: &bitcoin::Transaction,
        status: withdrawal::Status,
    ) {
        let mut amounts = HashMap::<bitcoin::Address, (u64, Vec<[u8; 32]>)>::new();
        let mut prev_address: Option<bitcoin::Address> = None;
        for out in bundle.output.iter() {
            let address = bitcoin::Address::from_script(
                &out.script_pubkey,
                bitcoin::network::constants::Network::Regtest,
            );
            if let Some(ref address) = address {
                let (amount, wtids) = amounts.entry(address.clone()).or_insert((0, vec![]));
                *amount += out.value;
                prev_address = Some(address.clone());
            }
            if out.script_pubkey.is_op_return() {
                for instruction in out.script_pubkey.instructions() {
                    if let Ok(bitcoin::blockdata::script::Instruction::PushBytes(bytes)) =
                        instruction
                    {
                        if bytes.len() == 32 {
                            if let Some(ref address) = prev_address {
                                let (_, wtids) = amounts.get_mut(&address).unwrap();
                                wtids.push(bytes.try_into().unwrap());
                            }
                        }
                    }
                }
            }
        }
        for (address, (paid_out, wtids)) in amounts {
            // let mut burned = 0;
            // for wtid in wtids.iter() {
            //     burned += self.get_withdrawal(*wtid).unwrap().amount;
            // }
            // if paid_out != burned {
            //     continue;
            // }
            for wtid in wtids.iter() {
                self.set_withdrawal_status(*wtid, status)
                    .expect("withdrawal doesn't exist");
            }
        }
    }

    pub fn get_unspent_withdrawals(&self) -> Vec<([u8; 32], Vec<WithdrawalOutput>)> {
        let unspent_withdrawals = self
            .unspent_withdrawals
            .iter()
            .map(|wtid| wtid.expect("failed to get unspent withdrawals range"));
        let withdrawals = unspent_withdrawals
            .map(|(wtid, _)| {
                let wtid: [u8; 32] = wtid.as_ref().try_into().unwrap();
                let wt = self
                    .get_withdrawal(wtid)
                    .expect("failed to get unspent withdrawal by wtid");
                (wtid, wt)
            })
            .collect();
        withdrawals
    }

    pub fn add_withdrawal(
        &mut self,
        wtid: [u8; 32],
        withdrawal: &Vec<WithdrawalOutput>,
    ) -> Result<(), withdrawal::Error> {
        self.check_withdrawal(wtid);
        // Can't add a withdrawal if one with the same wtid already exists.
        if self.contains_withdrawal(wtid) {
            return Err(withdrawal::Error::WithdrawalAlreadyExists);
        }
        let withdrawal = bincode::serialize(&withdrawal).expect("failed to serialize withdrawal");
        let status = bincode::serialize(&withdrawal::Status::Unspent)
            .expect("failed to serialize withdrawal status");
        (&self.withdrawals, &self.withdrawal_statuses, &self.unspent_withdrawals)
            .transaction(|(withdrawals, withdrawal_statuses, unspent_withdrawals)| -> sled::transaction::ConflictableTransactionResult<_, sled::Error> {
                withdrawals.insert(&wtid, withdrawal.clone())?;
                withdrawal_statuses.insert(&wtid, status.clone())?;
                unspent_withdrawals.insert(&wtid, &[])?;
                Ok(())
            }).expect("failed to add a withdrawal");
        Ok(())
    }

    fn contains_withdrawal(&self, wtid: [u8; 32]) -> bool {
        self.withdrawals
            .contains_key(wtid)
            .expect("failed to check if withdrawal exists")
    }

    // Check db consistency for wtid, and panic if db is inconsistent.
    fn check_withdrawal(&self, wtid: [u8; 32]) {
        let wt_exists = self
            .withdrawals
            .contains_key(wtid)
            .expect("failed to check if withdrawal exists");
        let wt_status_exists = self
            .withdrawal_statuses
            .contains_key(wtid)
            .expect("failed to check if withdrawal status exists");
        if wt_exists && !wt_status_exists {
            panic!("withdrawal exists but withdrawal status doesn't");
        } else if wt_status_exists && !wt_exists {
            panic!("withdrawal status exists but withdrawal doesn't");
        }
        let status = self
            .withdrawal_statuses
            .get(wtid)
            .expect("failed to get withdrawal status")
            .map(|s| bincode::deserialize(&s).expect("failed to deserialize withdrawal status"));
        let unspent_wt_exists = self
            .unspent_withdrawals
            .contains_key(wtid)
            .expect("failed to check if wtid is in unspent withdrawals");
        if status == Some(withdrawal::Status::Unspent) && !unspent_wt_exists {
            panic!("withdrawal is unspent but it is not in unspent withdrawals key value store");
        }
        if status != Some(withdrawal::Status::Unspent) && unspent_wt_exists {
            panic!("withdrawal is not unspent but it is in unspent withdrawals key value store");
        }
    }

    pub fn set_withdrawal_status(
        &mut self,
        wtid: [u8; 32],
        status: withdrawal::Status,
    ) -> Result<(), withdrawal::Error> {
        self.check_withdrawal(wtid);
        // Can only update status of an already existing withdrawal.
        if !self.contains_withdrawal(wtid) {
            return Err(withdrawal::Error::WithdrawalDoesntExist);
        }
        (&self.withdrawal_statuses, &self.unspent_withdrawals)
            .transaction(|(withdrawal_statuses, unspent_withdrawals)| -> sled::transaction::ConflictableTransactionResult<_, sled::Error> {
                match status {
                    withdrawal::Status::Unspent => {
                        unspent_withdrawals.insert(&wtid, &[])?
                    },
                    _ => {
                        unspent_withdrawals.remove(&wtid)?
                    },
                };
                let status_bin = bincode::serialize(&status).expect("failed to serialize withdrawal status");
                withdrawal_statuses.insert(&wtid, status_bin.clone())?;
                Ok(())
            })
            .expect("failed to update withdrawal status");
        Ok(())
    }

    pub fn get_withdrawal(&self, wtid: [u8; 32]) -> Option<Vec<WithdrawalOutput>> {
        self.withdrawals
            .get(wtid)
            .expect("failed to get withdrawal")
            .map(|withdrawal| {
                bincode::deserialize::<Vec<WithdrawalOutput>>(withdrawal.as_ref())
                    .expect("failed to deserialize withdrawal")
            })
    }

    pub fn get_withdrawal_status(&self, wtid: [u8; 32]) -> Option<withdrawal::Status> {
        self.withdrawal_statuses
            .get(wtid)
            .expect("failed to get withdrawal status")
            .map(|withdrawal_status| {
                bincode::deserialize(withdrawal_status.as_ref())
                    .expect("failed to deserialize withdrawal status")
            })
    }

    pub fn get_deposit(&self, index: usize) -> Option<Deposit> {
        let index: [u8; 4] = (index as u32).to_be_bytes();
        self.deposits
            .get(index)
            .expect("failed to get deposit")
            .map(|deposit| {
                bincode::deserialize(deposit.as_ref()).expect("failed to deserialize deposit")
            })
    }

    pub fn deposits_range(&self, start: usize, end: usize) -> Vec<(usize, Deposit)> {
        let start: [u8; 4] = (start as u32).to_be_bytes();
        let end: [u8; 4] = (end as u32).to_be_bytes();
        self.deposits
            .range(start..=end)
            .map(|item| {
                let (index, deposit) = item.expect("failed to get deposit range");
                (
                    BigEndian::read_u32(index.as_ref()) as usize,
                    bincode::deserialize::<Deposit>(deposit.as_ref())
                        .expect("failed to deserialize deposit"),
                )
            })
            .collect()
    }

    pub fn deposits_since(&self, start: usize) -> Vec<(usize, Deposit)> {
        let start: [u8; 4] = (start as u32).to_be_bytes();
        self.deposits
            .range(start..)
            .map(|item| {
                let (index, deposit) = item.expect("failed to get deposit range");
                (
                    BigEndian::read_u32(index.as_ref()) as usize,
                    bincode::deserialize::<Deposit>(deposit.as_ref())
                        .expect("failed to deserialize deposit"),
                )
            })
            .collect()
    }

    pub fn update_deposits(&self, deposits: &[Deposit]) {
        let sorted_deposits = DB::sort_deposits(deposits);
        let (last_index, last_deposit) = match self.get_last_deposit() {
            Some((last_index, last_deposit)) => (Some(last_index), Some(last_deposit)),
            None => (None, None),
        };
        let mut index = match last_index {
            Some(last_index) => last_index + 1,
            None => 0,
        };
        let mut deposits_batch = sled::Batch::default();
        let deposit_iter = sorted_deposits
            .iter()
            .skip_while(|deposit| match &last_deposit {
                Some(last_deposit) => !last_deposit.is_spent_by(deposit),
                None => false,
            });
        for deposit in deposit_iter {
            deposits_batch.insert(
                &(index as u32).to_be_bytes(),
                bincode::serialize(&deposit).expect("failed to serialize deposit"),
            );
            index += 1;
        }
        self.deposits
            .apply_batch(deposits_batch)
            .expect("failed to update deposits");
    }

    pub fn sort_deposits(deposits: &[Deposit]) -> Vec<Deposit> {
        if deposits.len() == 0 {
            return vec![];
        }
        let mut outpoint_to_deposit = HashMap::<OutPoint, Deposit>::new();
        let mut spent_by = HashMap::<OutPoint, OutPoint>::new();
        for deposit in deposits {
            outpoint_to_deposit.insert(deposit.outpoint(), deposit.clone());
        }
        let mut first_outpoint: Option<OutPoint> = None;
        for deposit in deposits {
            let input_outpoints = deposit
                .tx
                .input
                .iter()
                .filter(|input| outpoint_to_deposit.contains_key(&input.previous_output))
                .map(|input| input.previous_output)
                .collect::<Vec<OutPoint>>();
            if input_outpoints.len() == 1 {
                spent_by.insert(input_outpoints[0], deposit.outpoint());
            } else if input_outpoints.len() == 0 {
                first_outpoint = Some(deposit.outpoint());
            } else {
                panic!("Invalid deposit transaction - input spends more than one previous CTIP");
            }
        }
        let mut sorted_deposits = Vec::<Deposit>::new();
        if let Some(first_outpoint) = first_outpoint {
            sorted_deposits.push(outpoint_to_deposit[&first_outpoint].clone());
        }
        let first_deposit = &sorted_deposits[0];
        let mut deposit_outpoint = first_deposit.outpoint();
        while let Some(next) = spent_by.get(&deposit_outpoint) {
            if let Some(dep) = outpoint_to_deposit.get(next) {
                sorted_deposits.push(dep.clone());
                deposit_outpoint = dep.outpoint();
            }
        }
        sorted_deposits
    }

    pub fn get_last_deposit(&self) -> Option<(usize, Deposit)> {
        self.deposits
            .last()
            .expect("failed to get last deposit")
            .map(|(index, deposit)| {
                (
                    BigEndian::read_u32(index.as_ref()) as usize,
                    bincode::deserialize(deposit.as_ref()).expect("failed to deserialize deposit"),
                )
            })
    }

    pub fn remove_last_deposit(&self) {
        self.deposits
            .pop_max()
            .expect("failed to remove last deposit");
    }
}
