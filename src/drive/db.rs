use super::deposit::{Deposit, Output};
use super::withdrawal::WithdrawalOutput;
use bitcoin::blockdata::transaction::OutPoint;
use bitcoin::blockdata::{
    opcodes, script,
    transaction::{Transaction, TxIn, TxOut},
};
use bitcoin::hash_types::{ScriptHash, Txid};
use bitcoin::hashes::Hash;
use bitcoin::util::amount::Amount;
use bitcoin::util::psbt::serialize::Serialize;
use byteorder::{BigEndian, ByteOrder};
use sled::transaction::{abort, ConflictableTransactionError, TransactionError};
use sled::Transactional;
use std::collections::{HashMap, HashSet};

const DEPOSITS: &[u8] = b"deposits";
const DEPOSIT_BALANCES: &[u8] = b"deposit_balances";
const UNBALANCED_DEPOSITS: &[u8] = b"unbalanced_deposits";

const OUTPOINT_TO_WITHDRAWAL: &[u8] = b"outpoint_to_withdrawal";
const SPENT_OUTPOINTS: &[u8] = b"spent_outpoints";
const UNSPENT_OUTPOINTS: &[u8] = b"unspent_outpoints";

const BUNDLE_HASH_TO_INPUTS: &[u8] = b"bundle_hash_to_inputs";
const FAILED_BUNDLE_HASHES: &[u8] = b"failed_bundle_hashes";
const SPENT_BUNDLE_HASHES: &[u8] = b"spent_bundle_hashes";

pub struct DB {
    db: sled::Db,
    deposits: sled::Tree,
    deposit_balances: sled::Tree,
    unbalanced_deposits: sled::Tree,
    outpoint_to_withdrawal: sled::Tree,
    spent_outpoints: sled::Tree,
    unspent_outpoints: sled::Tree,
    pub bundle_hash_to_inputs: sled::Tree,

    // Failed and spent bundle hashes that we have already seen.
    failed_bundle_hashes: sled::Tree,
    spent_bundle_hashes: sled::Tree,
}

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

        let outpoint_to_withdrawal = db
            .open_tree(OUTPOINT_TO_WITHDRAWAL)
            .expect("couldn't open outpoint to withdrawal key value store");

        let spent_outpoints = db
            .open_tree(SPENT_OUTPOINTS)
            .expect("couldn't open spent outpoints key value store");

        let unspent_outpoints = db
            .open_tree(UNSPENT_OUTPOINTS)
            .expect("couldn't open unspent outpoints key value store");

        let bundle_hash_to_inputs = db
            .open_tree(BUNDLE_HASH_TO_INPUTS)
            .expect("couldn't open bundle hash to inputs key value store");

        let failed_bundle_hashes = db
            .open_tree(FAILED_BUNDLE_HASHES)
            .expect("couldn't open failed bundle hashes key value store");

        let spent_bundle_hashes = db
            .open_tree(SPENT_BUNDLE_HASHES)
            .expect("couldn't open spent bundle hashes key value store");

        DB {
            db,
            deposits,
            deposit_balances,
            unbalanced_deposits,
            outpoint_to_withdrawal,
            spent_outpoints,
            unspent_outpoints,
            bundle_hash_to_inputs,
            failed_bundle_hashes,
            spent_bundle_hashes,
        }
    }

    pub fn flush(&mut self) -> Result<usize, sled::Error> {
        self.db.flush()
    }

    pub fn get_deposit_outputs(&self) -> Vec<Output> {
        {
            let withdrawals: HashMap<String, WithdrawalOutput> = self
                .outpoint_to_withdrawal
                .iter()
                .map(|item| {
                    let (outpoint, withdrawal) = item.unwrap();
                    let outpoint = hex::encode(outpoint);
                    let withdrawal = bincode::deserialize::<WithdrawalOutput>(&withdrawal).unwrap();
                    (outpoint, withdrawal)
                })
                .collect();
            dbg!(withdrawals);
            let deposit_balances: HashMap<String, (u64, u64)> = self
                .deposit_balances
                .iter()
                .map(|item| {
                    let (address, balance) = item.unwrap();
                    let address = String::from_utf8(address.to_vec()).unwrap();
                    let (side_balance, main_balance) = bincode::deserialize::<(u64, u64)>(&balance)
                        .expect("failed to deserialize deposit balance");
                    (address, (side_balance, main_balance))
                })
                .collect();
            dbg!(deposit_balances);
        }
        let outs = self
            .unbalanced_deposits
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

    pub fn connect_withdrawals(&mut self, withdrawals: HashMap<Vec<u8>, WithdrawalOutput>) -> bool {
        (&self.outpoint_to_withdrawal, &self.unspent_outpoints)
            .transaction(|(outpoint_to_withdrawal, unspent_outpoints)| {
                for (outpoint, withdrawal) in withdrawals.iter() {
                    dbg!(hex::encode(outpoint.as_slice()), &withdrawal);
                    let outpoint = outpoint.as_slice();
                    let withdrawal = bincode::serialize(&withdrawal).or_else(abort)?;
                    outpoint_to_withdrawal.insert(outpoint, withdrawal)?;
                    unspent_outpoints.insert(outpoint, &[])?;
                }
                Ok(())
            })
            .is_ok()
    }

    // FIXME: It should be impossible to disconnect spent withdrawals if on
    // mainchain the bundle that spends the corresponding outpoints is not
    // disconnected as well.
    pub fn disconnect_withdrawals(&mut self, outpoints: Vec<Vec<u8>>) -> bool {
        (&self.outpoint_to_withdrawal, &self.unspent_outpoints, &self.spent_outpoints)
            .transaction(|(outpoint_to_withdrawal, unspent_outpoints, spent_outpoints)| -> sled::transaction::ConflictableTransactionResult<()> {
                for outpoint in outpoints.iter() {
                    let outpoint = outpoint.as_slice();
                    outpoint_to_withdrawal.remove(outpoint)?;
                    unspent_outpoints.remove(outpoint)?;
                    spent_outpoints.remove(outpoint)?;
                }
                Ok(())
            })
            .is_ok()
    }

    pub fn connect_deposit_outputs(
        &mut self,
        outputs: impl Iterator<Item = Output>,
        just_check: bool,
    ) -> bool {
        println!("connect_deposit_outputs");
        let mut side_balances = HashMap::<String, u64>::new();
        for output in outputs {
            let amount = side_balances.entry(output.address.clone()).or_insert(0);
            *amount += output.amount;
        }
        let result = (&self.deposit_balances, &self.unbalanced_deposits).transaction(
            |(deposit_balances, unbalanced_deposits)| {
                for (address, side_delta) in side_balances.iter() {
                    if let Some(balance) = deposit_balances.get(address.as_bytes())? {
                        let (old_side_balance, main_balance) =
                            bincode::deserialize::<(u64, u64)>(&balance)
                                .map_err(ConnectError::from)
                                .or_else(abort)?;
                        println!(
                            "main balance is {}. added {} to old side balance {}",
                            main_balance, side_delta, old_side_balance
                        );
                        let new_side_balance = old_side_balance + side_delta;
                        if new_side_balance != main_balance {
                            return abort(ConnectError::Unbalanced);
                        }
                        let new_balance = (new_side_balance, main_balance);
                        dbg!(new_balance);
                        let new_balance = bincode::serialize(&new_balance)
                            .map_err(ConnectError::from)
                            .or_else(abort)?;
                        deposit_balances.insert(address.as_bytes(), new_balance)?;
                        unbalanced_deposits.remove(address.as_bytes())?;
                    } else {
                        return abort(ConnectError::NoAddress);
                    }
                }
                if just_check {
                    return abort(ConnectError::JustChecking);
                }
                Ok(())
            },
        );
        dbg!(&result);
        if let Err(TransactionError::Abort(ConnectError::JustChecking)) = result {
            return true;
        }
        result.is_ok()
    }

    pub fn disconnect_deposit_outputs(
        &mut self,
        outputs: impl Iterator<Item = Output>,
        just_check: bool,
    ) -> bool {
        println!("disconnect_deposit_outputs");
        let mut side_balances = HashMap::<String, u64>::new();
        for output in outputs {
            let amount = side_balances.entry(output.address.clone()).or_insert(0);
            *amount += output.amount;
        }
        let result = (&self.deposit_balances, &self.unbalanced_deposits).transaction(
            |(deposit_balances, unbalanced_deposits)| {
                for (address, side_delta) in side_balances.iter() {
                    if let Some(balance) = deposit_balances.get(address.as_bytes())? {
                        let (old_side_balance, main_balance) =
                            bincode::deserialize::<(u64, u64)>(&balance)
                                .map_err(DisconnectError::from)
                                .or_else(abort)?;
                        let new_side_balance = old_side_balance - side_delta;
                        if !just_check {
                            let new_balance = (new_side_balance, main_balance);
                            let new_balance = bincode::serialize(&new_balance)
                                .map_err(DisconnectError::from)
                                .or_else(abort)?;
                            deposit_balances.insert(address.as_bytes(), new_balance)?;
                            unbalanced_deposits.insert(address.as_bytes(), &[])?;
                        }
                    } else {
                        return abort(DisconnectError::JustChecking);
                    }
                }
                Ok(())
            },
        );
        if let Err(TransactionError::Abort(DisconnectError::JustChecking)) = result {
            return true;
        }
        result.is_ok()
    }

    pub fn update_deposits(&self, deposits: &[Deposit]) {
        dbg!("update_deposits");
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
        dbg!(start_index);
        // We get an index -> deposit HashMap, by zipping a sequence of indices
        // starting at start_index with deposits not in the database.
        let sorted_deposits = sorted_deposits
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
            })
            .enumerate();
        let mut balances = HashMap::<String, Amount>::new();
        let mut prev_amount = last_deposit
            .as_ref()
            .map(|deposit| deposit.amount())
            .unwrap_or(Amount::ZERO);
        let mut deposits_batch = sled::Batch::default();
        for (index, deposit) in sorted_deposits {
            let index = start_index + index;
            deposits_batch.insert(
                &(index as u32).to_be_bytes(),
                bincode::serialize(&deposit).expect("failed to serialize deposit"),
            );
            if deposit.amount() < prev_amount {
                continue;
            }
            println!(
                "index {} added {} to {}",
                index,
                deposit.amount() - prev_amount,
                deposit.strdest
            );
            let balance = balances
                .entry(deposit.strdest.clone())
                .or_insert(Amount::ZERO);
            *balance += deposit.amount() - prev_amount;
            prev_amount = deposit.amount();
        }

        dbg!(&balances);
        (
            &self.deposits,
            &self.deposit_balances,
            &self.unbalanced_deposits,
        )
            .transaction(|(deposits, deposit_balances, unbalanced_deposits)| -> sled::transaction::ConflictableTransactionResult<(), bincode::Error> {
                deposits.apply_batch(&deposits_batch)?;

                for (address, main_amount) in balances.iter() {
                    println!("iteration of update deposits for {} with {}", address, main_amount);
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
                    dbg!(&address, &balance);
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

    pub fn is_outpoint_spent(&self, outpoint: Vec<u8>) -> bool {
        self.spent_outpoints
            .contains_key(outpoint.as_slice())
            .unwrap()
    }

    pub fn vote_bundle(&mut self, txid: &Txid) {
        let inputs = self.get_inputs(txid);
        (&self.unspent_outpoints, &self.spent_outpoints).transaction(
            |(unspent_outpoints, spent_outpoints)| -> sled::transaction::ConflictableTransactionResult<(), ()> {
                for input in inputs.iter() {
                    unspent_outpoints.remove(input.as_slice())?;
                    spent_outpoints.insert(input.as_slice(), &[])?;
                }
                Ok(())
            },
        ).expect("failed to mark voting bundle inputs as spent");
    }

    pub fn spend_bundle(&mut self, txid: &Txid) {
        let inputs = self.get_inputs(txid);
        (&self.spent_bundle_hashes, &self.unspent_outpoints, &self.spent_outpoints).transaction(
            |(spent_bundle_hashes, unspent_outpoints, spent_outpoints)| -> sled::transaction::ConflictableTransactionResult<(), ()> {
                spent_bundle_hashes.insert(txid.as_inner(), &[])?;
                for input in inputs.iter() {
                    unspent_outpoints.remove(input.as_slice())?;
                    spent_outpoints.insert(input.as_slice(), &[])?;
                }
                Ok(())
            },
        ).expect("failed to mark bundle as spent");
    }

    pub fn fail_bundle(&mut self, txid: &Txid) {
        let inputs = self.get_inputs(txid);
        (&self.failed_bundle_hashes, &self.unspent_outpoints, &self.spent_outpoints).transaction(
            |(failed_bundle_hashes, unspent_outpoints, spent_outpoints)| -> sled::transaction::ConflictableTransactionResult<(), ()> {
                failed_bundle_hashes.insert(txid.as_inner(), &[])?;
                for input in inputs.iter() {
                    spent_outpoints.remove(input.as_slice())?;
                    unspent_outpoints.insert(input.as_slice(), &[])?;
                }
                Ok(())
            },
        ).expect("failed to mark bundle as failed");
    }

    pub fn get_spent_bundle_hashes(&self) -> HashSet<Txid> {
        self.spent_bundle_hashes
            .iter()
            .map(|item| {
                let (txid, _) = item.unwrap();
                let mut txid_inner: [u8; 32] = Default::default();
                txid_inner.copy_from_slice(&txid);
                Txid::from_inner(txid_inner)
            })
            .collect()
    }

    pub fn get_failed_bundle_hashes(&self) -> HashSet<Txid> {
        self.failed_bundle_hashes
            .iter()
            .map(|item| {
                let (txid, _) = item.unwrap();
                let mut txid_inner: [u8; 32] = Default::default();
                txid_inner.copy_from_slice(&txid);
                Txid::from_inner(txid_inner)
            })
            .collect()
    }

    pub fn get_inputs(&mut self, txid: &Txid) -> Vec<Vec<u8>> {
        let hash = txid.into_inner();
        self.bundle_hash_to_inputs
            .get(hash)
            .expect("failed to get bundle inputs")
            .map(|inputs| {
                bincode::deserialize::<Vec<Vec<u8>>>(&inputs)
                    .expect("failed to deserialize bundle inputs")
            })
            .unwrap_or(vec![])
    }

    pub fn create_bundle(&mut self) -> Option<bitcoin::Transaction> {
        let withdrawals = self.unspent_outpoints.iter().map(|item| {
            let (outpoint, _) = item.unwrap();
            let withdrawal = self.outpoint_to_withdrawal.get(&outpoint).unwrap().unwrap();
            let withdrawal = bincode::deserialize::<WithdrawalOutput>(&withdrawal).unwrap();
            (outpoint.to_vec(), withdrawal)
        });

        // Aggregate all outputs by destination.
        // destination -> (amount, mainchain fee)
        let mut dest_to_withdrawal = HashMap::<[u8; 20], (u64, u64)>::new();
        let mut outpoints = vec![];
        for withdrawal in withdrawals {
            let (outpoint, output) = withdrawal;
            outpoints.push(outpoint);
            let (amount, mainchain_fee) = dest_to_withdrawal.entry(output.dest).or_insert((0, 0));
            // Add up all amounts.
            *amount += output.amount;
            // Set the maximum mainchain fee.
            *mainchain_fee = std::cmp::max(*mainchain_fee, output.mainchain_fee);
        }
        // HashMap used for aggregation into a vector.
        let mut outputs: Vec<WithdrawalOutput> = dest_to_withdrawal
            .into_iter()
            .map(|(dest, (amount, mainchain_fee))| WithdrawalOutput {
                dest,
                amount,
                mainchain_fee,
            })
            .collect();
        // Sort the outputs in descending order.
        //
        // Outputs are sorted first by mainchain_fee, then by dest, and then by
        // amount (see Ord implementation in withdrawal.rs).
        //
        // Because there are no outputs with repeating dest in this vector,
        // amount will never affect the order only mainchain_fee and dest.
        outputs.sort_by_key(|a| std::cmp::Reverse(*a));
        let mut fee = 0;
        let mut bundle_outputs = vec![];
        for output in outputs {
            let script_hash = ScriptHash::from_inner(output.dest);
            let address = bitcoin::Address {
                payload: bitcoin::util::address::Payload::ScriptHash(script_hash),
                network: bitcoin::Network::Testnet,
            };
            let bundle_output = bitcoin::TxOut {
                value: output.amount,
                script_pubkey: address.script_pubkey(),
            };
            bundle_outputs.push(bundle_output);
            fee += output.mainchain_fee;
            const MAX_BUNDLE_OUTPUTS: usize = 10;
            if bundle_outputs.len() >= MAX_BUNDLE_OUTPUTS {
                break;
            }
        }
        let txin = TxIn {
            script_sig: script::Builder::new()
                // OP_FALSE == OP_0
                .push_opcode(opcodes::OP_FALSE)
                .into_script(),
            ..TxIn::default()
        };
        // Create return dest output.
        // The destination string for the change of a WT^
        const SIDECHAIN_WTPRIME_RETURN_DEST: &[u8] = b"D";
        let script = script::Builder::new()
            .push_opcode(opcodes::all::OP_RETURN)
            .push_slice(SIDECHAIN_WTPRIME_RETURN_DEST)
            .into_script();
        let return_dest_txout = TxOut {
            value: 0,
            script_pubkey: script,
        };
        // Create mainchain fee output.
        let script = script::Builder::new()
            .push_opcode(opcodes::all::OP_RETURN)
            .push_slice(fee.to_le_bytes().as_ref())
            .into_script();
        let mainchain_fee_txout = TxOut {
            value: 0,
            script_pubkey: script,
        };
        let bundle = bitcoin::Transaction {
            version: 2,
            lock_time: 0,
            input: vec![txin],
            output: [vec![return_dest_txout, mainchain_fee_txout], bundle_outputs].concat(),
        };
        {
            let hash = bundle.txid();
            let hash = hash.into_inner();
            let inputs = outpoints;
            let inputs = bincode::serialize(&inputs).expect("failed to serialize bundle inputs");
            self.bundle_hash_to_inputs
                .insert(hash, inputs)
                .expect("failed to write bundle inputs");
        }
        Some(bundle)
    }
}

#[derive(Debug)]
enum ConnectError {
    Bincode(bincode::Error),
    Unbalanced,
    NoAddress,
    JustChecking,
}

enum DisconnectError {
    Bincode(bincode::Error),
    JustChecking,
}

impl From<bincode::Error> for ConnectError {
    fn from(err: bincode::Error) -> ConnectError {
        ConnectError::Bincode(err)
    }
}

impl From<bincode::Error> for DisconnectError {
    fn from(err: bincode::Error) -> DisconnectError {
        DisconnectError::Bincode(err)
    }
}
