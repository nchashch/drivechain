use super::deposit::{Deposit, Output};
use super::withdrawal;
use super::withdrawal::WithdrawalOutput;
use bitcoin::blockdata::transaction::OutPoint;
use bitcoin::util::amount::Amount;
use byteorder::{BigEndian, ByteOrder};
use sled::transaction::ConflictableTransactionError;
use sled::Transactional;
use std::collections::{HashMap, HashSet};

// (balanced|unbalanced)str dest -> (side amount, main amount)

const DEPOSITS: &[u8] = b"deposits";
const DEPOSIT_BALANCES: &[u8] = b"deposit_balances";
const UNBALANCED_DEPOSITS: &[u8] = b"unbalanced_deposits";

// outpoint -> (mainchain destination, mainchain fee, amount)
const OUTPOINT_TO_WITHDRAWAL: &[u8] = b"outpoint_to_withdrawal";
const BUNDLE_HASh_TO_INPUTS: &[u8] = b"bundle_hash_to_inputs";

pub struct DB {
    pub db: sled::Db,
    deposits: sled::Tree,
    deposit_balances: sled::Tree,
    unbalanced_deposits: sled::Tree,
}

// no bundle
// pending
//
// max outputs
// max height

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

        let deposit_balances = db
            .open_tree(DEPOSIT_BALANCES)
            .expect("couldn't open deposit balances key value store");

        let unbalanced_deposits = db
            .open_tree(UNBALANCED_DEPOSITS)
            .expect("couldn't open unbalanced deposits key value store");

        DB {
            db,
            deposits,
            deposit_balances,
            unbalanced_deposits,
        }
    }

    pub fn get_deposit_outputs(&self) -> Vec<Output> {
        let outs = self.unbalanced_deposits
            .iter()
            .map(|address| {
                let (address, _) = address.expect("failed to get unbalanced deposit address");
                let balance = self
                    .deposit_balances
                    .get(&address)
                    .expect("failed to get balance")
                    .expect("unbalanced output doesn't exist");
                let (side_balance, main_balance) = bincode::deserialize::<(u64, u64)>(&balance)
                    .expect("failed to deserialize deposit balance");
                Output {
                    address: String::from_utf8(address.to_vec())
                        .expect("failed to decode address string"),
                    amount: main_balance - side_balance,
                }
            })
            .collect();
        dbg!(&outs);
        outs
    }

    pub fn connect_side_outputs(
        &mut self,
        outputs: impl Iterator<Item = Output>,
        just_check: bool,
    ) -> bool {
        let mut side_balances = HashMap::<String, u64>::new();
        for output in outputs {
            let amount = side_balances.entry(output.address.clone()).or_insert(0);
            *amount += output.amount;
        }
        (&self.deposit_balances, &self.unbalanced_deposits)
            .transaction(|(deposit_balances, unbalanced_deposits)| -> sled::transaction::ConflictableTransactionResult<bool, bincode::Error> {
                for (address, side_delta) in side_balances.iter() {
                    if let Some(balance) = deposit_balances.get(address.as_bytes())? {
                        let (old_side_balance, main_balance) =
                            bincode::deserialize::<(u64, u64)>(&balance).map_err(ConflictableTransactionError::Abort)?;
                        let new_side_balance = old_side_balance + side_delta;
                        if new_side_balance != main_balance {
                            return Ok(false);
                        }
                        if !just_check {
                            let new_balance = (new_side_balance, main_balance);
                            let new_balance = bincode::serialize(&new_balance).map_err(ConflictableTransactionError::Abort)?;
                            deposit_balances
                                .insert(address.as_bytes(), new_balance)?;
                            unbalanced_deposits.remove(address.as_bytes())?;
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            })
            .unwrap()
    }

    pub fn disconnect_side_outputs(
        &mut self,
        outputs: impl Iterator<Item = Output>,
        just_check: bool,
    ) -> bool {
        let mut side_balances = HashMap::<String, u64>::new();
        for output in outputs {
            let amount = side_balances.entry(output.address.clone()).or_insert(0);
            *amount += output.amount;
        }
        (&self.deposit_balances, &self.unbalanced_deposits)
            .transaction(|(deposit_balances, unbalanced_deposits)| -> sled::transaction::ConflictableTransactionResult<bool, bincode::Error> {
                for (address, side_delta) in side_balances.iter() {
                    if let Some(balance) = deposit_balances.get(address.as_bytes())? {
                        let (old_side_balance, main_balance) =
                            bincode::deserialize::<(u64, u64)>(&balance).map_err(ConflictableTransactionError::Abort)?;
                        let new_side_balance = old_side_balance - side_delta;
                        if !just_check {
                            let new_balance = (new_side_balance, main_balance);
                            let new_balance = bincode::serialize(&new_balance).map_err(ConflictableTransactionError::Abort)?;
                            deposit_balances
                                .insert(address.as_bytes(), new_balance)?;
                            unbalanced_deposits.insert(address.as_bytes(), &[])?;
                        }
                    } else {
                        return Ok(false);
                    }
                }
                Ok(true)
            }).unwrap()
    }

    pub fn update_deposits(&self, deposits: &[Deposit]) {
        // New deposits are sorted in CTIP order.
        let sorted_deposits = DB::sort_deposits(deposits);
        // We get the last deposit stored in the db.
        let (last_index, last_deposit) = match self.get_last_deposit() {
            Some((last_index, last_deposit)) => (Some(last_index), Some(last_deposit)),
            None => (None, None),
        };
        // We get the starting index for new deposits, it is either:
        let start_index = match last_index {
            // one after the last index
            Some(last_index) => last_index + 1,
            // or if there are no deposits in db it is 0.
            None => 0,
        };
        // We get an index -> deposit HashMap, by zipping a sequence of indices
        // starting at start_index with deposits not in the database.
        let index_to_deposit: HashMap<usize, Deposit> = (start_index..)
            .zip(
                sorted_deposits
                    .into_iter()
                    // We skip new deposits until we find one that "spends" the
                    // last deposit, this way we skip all deposits that are
                    // already in the db.
                    //
                    // The deposit that spends last_deposit comes right after
                    // last_deposit, so it's index = start_index.
                    //
                    // If there are no new deposits that spend last_deposit then
                    // all deposits will be skipped and we will get an empty
                    // HashMap.
                    .skip_while(|deposit| match &last_deposit {
                        Some(last_deposit) => !last_deposit.is_spent_by(deposit),
                        // If there are no deposits in db we don't skip any
                        // deposits.
                        None => false,
                    }),
            )
            .collect();
        let mut balances = HashMap::<String, Amount>::new();
        let mut prev_deposit = last_deposit;
        let mut deposits_batch = sled::Batch::default();
        for (index, deposit) in index_to_deposit {
            deposits_batch.insert(
                &(index as u32).to_be_bytes(),
                bincode::serialize(&deposit).expect("failed to serialize deposit"),
            );
            let prev_amount = prev_deposit.as_ref().map(|deposit| deposit.amount());
            let prev_amount = prev_amount.unwrap_or(Amount::ZERO);
            if deposit.amount() < prev_amount {
                continue;
            }
            let balance = balances
                .entry(deposit.strdest.clone())
                .or_insert(Amount::ZERO);
            *balance += deposit.amount() - prev_amount;
            prev_deposit = Some(deposit);
        }

        (
            &self.deposits,
            &self.deposit_balances,
            &self.unbalanced_deposits,
        )
            .transaction(|(deposits, deposit_balances, unbalanced_deposits)| -> sled::transaction::ConflictableTransactionResult<(), bincode::Error> {
                deposits.apply_batch(&deposits_batch)?;

                for (address, main_amount) in balances.iter() {
                    let balance = deposit_balances
                        .get(address.as_bytes())?;
                    let balance = match balance {
                        Some(balance) => Some(bincode::deserialize::<(u64, u64)>(&balance).map_err(ConflictableTransactionError::Abort)?),
                        None => None,
                    };
                    let balance = match balance {
                        Some(balance) => (balance.0, balance.1 + main_amount.as_sat()),
                        None => (0, main_amount.as_sat()),
                    };
                    let balance = bincode::serialize(&balance).map_err(ConflictableTransactionError::Abort)?;
                    deposit_balances.insert(address.as_bytes(), balance)?;
                    unbalanced_deposits.insert(address.as_bytes(), &[])?;
                }
                Ok(())
            })
            .unwrap();
    }

    pub fn sort_deposits(deposits: &[Deposit]) -> Vec<Deposit> {
        if deposits.is_empty() {
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
            } else if input_outpoints.is_empty() {
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
