// FIXME: Write down how this all works in plain english.
// FIXME: This file is ill structured and too long. Restructure it and document
// it thoroughly.
// FIXME: Add unit tests for every function.
// FIXME: Use SQLite instead of sled.
use super::deposit::{Deposit, DepositUpdate, MainDeposit};
use super::withdrawal::Withdrawal;
use bitcoin::blockdata::{
    opcodes, script,
    transaction::{OutPoint, TxIn, TxOut},
};
use bitcoin::hash_types::{BlockHash, ScriptHash, Txid};
use bitcoin::hashes::Hash;
use bitcoin::util::amount::Amount;
use byteorder::{BigEndian, ByteOrder};
use log::{error, trace};
use rusqlite::{Connection, OptionalExtension, Result};
use sled::transaction::{abort, TransactionError};
use sled::Transactional;
use std::collections::{HashMap, HashSet};

#[cfg(test)]
use fake::{Dummy, Fake, Faker};
#[cfg(test)]
use quickcheck_macros::quickcheck;

// Current sidechain block height.
const SIDE_BLOCK_HEIGHT: &[u8] = b"side_block_height";
// Height at which last bundle has either failed or been spent.
const LAST_FAILED_BUNDLE_HEIGHT: &[u8] = b"last_failed_bundle_height";
// Mainchain block height of the last known bmm commitment.
const LAST_BMM_COMMITMENT_MAIN_BLOCK_HEIGHT: &[u8] = b"last_bmm_commitment_main_block_height";

#[derive(Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Outpoint(Vec<u8>);

impl Outpoint {
    fn bytes(&self) -> &[u8] {
        self.0.as_slice()
    }
    fn hex(&self) -> String {
        hex::encode(self.bytes())
    }
}
#[derive(Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Address(String);

// TODO: Implement proper transactions support in typed_sled or write another
// typed wrapper over sled.
pub struct DB {
    conn: Connection,
    db: sled::Db,
    // Index -> Deposit
    deposits: sled::Tree,
    // Address -> (u64, u64)
    deposit_balances: sled::Tree,
    // Address -> ()
    unbalanced_deposits: sled::Tree,
    // Outpoint -> Withdrawal
    outpoint_to_withdrawal: sled::Tree,
    // Outpoint -> ()
    spent_outpoints: sled::Tree,
    // Outpoint -> ()
    unspent_outpoints: sled::Tree,
    // Txid -> Vec<Outpoint>
    bundle_hash_to_inputs: sled::Tree,

    // Failed and spent bundle hashes that we have already seen.
    // Txid -> ()
    failed_bundle_hashes: sled::Tree,
    // Txid -> ()
    spent_bundle_hashes: sled::Tree,

    // Store for values like:
    // block_height
    // last_failed_bundle_height
    // last_valid_bmm_main_block_height
    // String -> u64
    values: sled::Tree,
}

impl DB {
    pub fn new<P: AsRef<std::path::Path> + std::fmt::Display>(path: P) -> Result<DB, Error> {
        trace!("creating drivechain database with path = {}", path);
        let conn = Connection::open_in_memory()?;
        conn.execute(
            "CREATE TABLE deposit (
            id INTEGER PRIMARY KEY,
            blockhash BLOB NOT NULL,
            txid BLOB NOT NULL,
            vout INTEGER NOT NULL,
            amount INTEGER NOT NULL,
            strdest TEXT NOT NULL
        )",
            (), // empty list of parameters.
        )?;
        conn.execute(
            "CREATE TABLE deposit_balance (
            address TEXT PRIMARY KEY,
            delta INTEGER NOT NULL
        )",
            (), // empty list of parameters.
        )?;
        conn.execute(
            "CREATE TABLE withdrawal (
            outpoint BLOB PRIMARY KEY,
            fee INTEGER NOT NULL,
            height INTEGER NOT NULL,
            dest BLOB NOT NULL,
            amount INTEGER NOT NULL,
            bundle BLOB
        )",
            (), // empty list of parameters.
        )?;
        let db = sled::open(path)?;
        let deposits = db.open_tree("deposits")?;
        let deposit_balances = db.open_tree("deposit_balances")?;
        let unbalanced_deposits = db.open_tree("unbalanced_deposits")?;
        let outpoint_to_withdrawal = db.open_tree("outpoint_to_withdrawal")?;
        let spent_outpoints = db.open_tree("spent_outpoints")?;
        let unspent_outpoints = db.open_tree("unspent_outpoints")?;
        let bundle_hash_to_inputs = db.open_tree("bundle_hash_to_inputs")?;
        let failed_bundle_hashes = db.open_tree("failed_bundle_hashes")?;
        let spent_bundle_hashes = db.open_tree("spent_bundle_hashes")?;
        let values = db.open_tree("values")?;
        trace!("drivechain database successfuly created");
        Ok(DB {
            conn,
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
            values,
        })
    }

    pub fn _get_deposit_outputs(&self) -> Result<HashMap<String, u64>, Error> {
        let mut deposits = self
            .conn
            .prepare("SELECT address, delta FROM deposit_balance WHERE delta > 0")?;
        let deposits = deposits
            .query_map([], |row| {
                let address: String = row.get(0)?;
                let amount: u64 = row.get(1)?;
                Ok((address, amount))
            })?
            .map(|deposit| deposit.unwrap())
            .collect();
        Ok(deposits)
    }

    pub fn get_deposit_outputs(&self) -> Result<Vec<Deposit>, Error> {
        trace!("getting unpaid deposit outputs from db");
        let deposit_outputs: Result<Vec<Deposit>, Error> = self
            .unbalanced_deposits
            .iter()
            .map(|address| {
                let (address, _) = address?;
                let balance = match self.deposit_balances.get(&address)? {
                    Some(balance) => balance,
                    None => return Err(Error::NoDeposit(String::from_utf8(address.to_vec())?)),
                };
                let (side_balance, main_balance) = bincode::deserialize::<(u64, u64)>(&balance)?;
                Ok(Deposit {
                    address: String::from_utf8(address.to_vec())?,
                    amount: main_balance - side_balance,
                })
            })
            .collect();
        match &deposit_outputs {
            Ok(outputs) => trace!("got {} unpaid deposit outputs", outputs.len()),
            Err(err) => trace!("failed to get deposit outputs with error = {}", err),
        };
        deposit_outputs
    }

    pub fn connect_block(
        &mut self,
        deposits: &[Deposit],
        withdrawals: &HashMap<Outpoint, Withdrawal>,
        refunds: &HashMap<Outpoint, u64>,
        // FIXME: Get rid of this flag.
        just_check: bool,
    ) -> Result<(), Error> {
        (
            // deposits
            &self.deposit_balances,
            &self.unbalanced_deposits,
            // withdrawals and refunds
            &self.outpoint_to_withdrawal,
            &self.unspent_outpoints,
            &self.spent_outpoints,
            &self.values,
        )
            .transaction(
                |(
                    // deposits
                    deposit_balances,
                    unbalanced_deposits,
                    // withdrawals
                    outpoint_to_withdrawal,
                    unspent_outpoints,
                    spent_outpoints,
                    values,
                )| {
                    trace!(
                        "connecting {} deposit outputs{}",
                        deposits.len(),
                        match just_check {
                            true => " (just checking)",
                            false => "",
                        }
                    );
                    let mut side_balances = HashMap::<String, u64>::new();
                    for output in deposits {
                        let amount = side_balances.entry(output.address.clone()).or_insert(0);
                        *amount += output.amount;
                    }
                    for (address, side_delta) in side_balances.iter() {
                        if let Some(balance) = deposit_balances.get(address.as_bytes())? {
                            let (old_side_balance, main_balance) =
                                bincode::deserialize::<(u64, u64)>(&balance)
                                    .map_err(Error::from)
                                    .or_else(abort)?;
                            trace!(
                                "main balance is {}. added {} to old side balance {}",
                                main_balance,
                                side_delta,
                                old_side_balance
                            );
                            let new_side_balance = old_side_balance + side_delta;
                            // FIXME: If a new deposit is added after a block was
                            // generated and before it was connected this will fail.
                            //
                            // This will happen if we:
                            //
                            // 1. generate a sidechain block and call attempt_bmm
                            // 2. create a new deposit
                            // 3. mine a mainchain block
                            // 4. call confirm_bmm
                            if new_side_balance != main_balance {
                                return abort(
                                    ConnectError::Unbalanced {
                                        address: address.clone(),
                                        side_balance: bitcoin::Amount::from_sat(new_side_balance),
                                        main_balance: bitcoin::Amount::from_sat(main_balance),
                                    }
                                    .into(),
                                );
                            }
                            let new_balance = (new_side_balance, main_balance);
                            let new_balance = bincode::serialize(&new_balance)
                                .map_err(Error::from)
                                .or_else(abort)?;
                            deposit_balances.insert(address.as_bytes(), new_balance)?;
                            unbalanced_deposits.remove(address.as_bytes())?;
                        } else {
                            return abort(ConnectError::NoAddress(address.clone()).into());
                        }
                    }
                    trace!("deposit outputs connected");

                    trace!("connecting {} withdrawals", withdrawals.len());
                    let height = match values.get(SIDE_BLOCK_HEIGHT)? {
                        Some(bytes) => {
                            let array: [u8; 8] = bytes
                                .as_ref()
                                .try_into()
                                .map_err(|err: std::array::TryFromSliceError| err.into())
                                .or_else(abort)?;
                            u64::from_be_bytes(array)
                        }
                        None => 0,
                    };
                    let height = height + 1;
                    values.insert(SIDE_BLOCK_HEIGHT, &height.to_be_bytes())?;
                    for (outpoint, withdrawal) in withdrawals.iter() {
                        let outpoint = outpoint.bytes();
                        let withdrawal = Withdrawal {
                            height,
                            ..*withdrawal
                        };
                        let withdrawal = bincode::serialize(&withdrawal)
                            .map_err(|err| err.into())
                            .or_else(abort)?;
                        outpoint_to_withdrawal.insert(outpoint, withdrawal)?;
                        unspent_outpoints.insert(outpoint, &[])?;
                    }
                    trace!("withdrawals connected");

                    // FIXME: Add separate refunded_outpoints store to allow
                    // checking spent bundle validity.
                    trace!("connecting {} refunds", refunds.len());
                    for (outpoint, amount) in refunds.iter() {
                        let outpoint = outpoint.bytes();
                        match outpoint_to_withdrawal.get(outpoint)? {
                            Some(withdrawal) => {
                                // It is unnecessary and inconvenient to check
                                // refund amounts here for sidechains using the
                                // UTXO model.
                                //
                                // But for sidechains using accounts model it is
                                // a lot easier to check refund amounts here, in
                                // the connect_block method.
                                //
                                // So we just gate this behind a feature.
                                //
                                // TODO: Figure out how to deal with this
                                // problem in a better way.
                                #[cfg(feature = "refund_amount_check")]
                                {
                                    let withdrawal =
                                        bincode::deserialize::<Withdrawal>(&withdrawal)
                                            .map_err(|err| err.into())
                                            .or_else(abort)?;
                                    if withdrawal.amount != *amount {
                                        return abort(
                                            ConnectError::WrongRefundAmount {
                                                outpoint: hex::encode(outpoint),
                                                actual_amount: withdrawal.amount,
                                                refunded_amount: *amount,
                                            }
                                            .into(),
                                        );
                                    }
                                }
                                // NOTE: Can't use is_outpoint_spent here,
                                // because it would deadlock the sled
                                // transaction.
                                if spent_outpoints.get(outpoint).map(|value| value.is_some())? {
                                    return abort(
                                        ConnectError::RefundedSpentOutpoint {
                                            outpoint: hex::encode(outpoint),
                                        }
                                        .into(),
                                    );
                                }
                            }
                            None => continue,
                        }
                        if unspent_outpoints.remove(outpoint)?.is_some() {
                            spent_outpoints.insert(outpoint, &[])?;
                        }
                    }
                    trace!("refunds connected");

                    if just_check {
                        // Don't commit changes to db if we are just checking.
                        trace!("block can be connected without errors");
                        return abort(ConnectError::JustChecking.into());
                    }
                    trace!("block connected");
                    Ok(())
                },
            )
            .map_err(|err| err.into())
            .or_else(|err| match err {
                Error::Connect(ConnectError::JustChecking) => Ok(()),
                err => Err(err),
            })
    }

    pub fn disconnect_block(
        &mut self,
        deposits: &[Deposit],
        withdrawals: &[Outpoint],
        refunds: &[Outpoint],
        // FIXME: Get rid of this flag.
        just_check: bool,
    ) -> Result<(), Error> {
        trace!(
            "disconnecting {} deposit outputs{}",
            deposits.len(),
            match just_check {
                true => " (just checking)",
                false => "",
            }
        );
        let mut side_balances = HashMap::<String, u64>::new();
        for output in deposits {
            let amount = side_balances.entry(output.address.clone()).or_insert(0);
            *amount += output.amount;
        }
        (
            // deposits
            &self.deposit_balances,
            &self.unbalanced_deposits,
            // withdrawals and refunds
            &self.outpoint_to_withdrawal,
            &self.unspent_outpoints,
            &self.spent_outpoints,
            &self.values,
        )
            .transaction(
                |(
                    // deposits
                    deposit_balances,
                    unbalanced_deposits,
                    // withdrawals and refunds
                    outpoint_to_withdrawal,
                    unspent_outpoints,
                    spent_outpoints,
                    values,
                )| {
                    for (address, side_delta) in side_balances.iter() {
                        if let Some(balance) = deposit_balances.get(address.as_bytes())? {
                            let (old_side_balance, main_balance) =
                                bincode::deserialize::<(u64, u64)>(&balance)
                                    .map_err(Error::from)
                                    .or_else(abort)?;
                            let new_side_balance = old_side_balance - side_delta;
                            if !just_check {
                                let new_balance = (new_side_balance, main_balance);
                                let new_balance = bincode::serialize(&new_balance)
                                    .map_err(Error::from)
                                    .or_else(abort)?;
                                deposit_balances.insert(address.as_bytes(), new_balance)?;
                                unbalanced_deposits.insert(address.as_bytes(), &[])?;
                            }
                        } else {
                            return abort(DisconnectError::NoDepositInDB(address.clone()).into());
                        }
                    }
                    trace!("deposit outputs disconnected");

                    // FIXME: It should be impossible to disconnect spent withdrawals if on
                    // mainchain the bundle that spends the corresponding outpoints is not
                    // disconnected as well.
                    //
                    // It might make sense to only create bundles from withdrawal outputs that
                    // have some minimum number of confirmations to make it harder to invalidate
                    // the current bundle by reorging the sidechain independently of mainchain.
                    //
                    // Or require that a sidechain never reorg unless there was a mainchain reorg.
                    trace!("disconnecting {} withdrawals", withdrawals.len());
                    let height = match values.get(SIDE_BLOCK_HEIGHT)? {
                        Some(bytes) => {
                            let array: [u8; 8] = bytes
                                .as_ref()
                                .try_into()
                                .map_err(|err: std::array::TryFromSliceError| err.into())
                                .or_else(abort)?;
                            u64::from_be_bytes(array)
                        }
                        None => return abort(DisconnectError::Genesis.into()),
                    };
                    let height = height - 1;
                    values.insert(SIDE_BLOCK_HEIGHT, &height.to_be_bytes())?;
                    for outpoint in withdrawals.iter() {
                        let outpoint = outpoint.bytes();
                        outpoint_to_withdrawal.remove(outpoint)?;
                        unspent_outpoints.remove(outpoint)?;
                        spent_outpoints.remove(outpoint)?;
                    }
                    trace!("withdrawals disconnected");

                    trace!("disconnecting {} refunds", refunds.len());
                    for outpoint in refunds.iter() {
                        let outpoint = outpoint.bytes();
                        if outpoint_to_withdrawal.get(outpoint)?.is_none() {
                            continue;
                        }
                        if spent_outpoints.remove(outpoint)?.is_some() {
                            unspent_outpoints.insert(outpoint, &[])?;
                        }
                    }
                    trace!("refunds disconnected");
                    if just_check {
                        trace!("block can be disconnected without errors");
                        return abort(DisconnectError::JustChecking.into());
                    }
                    trace!("block disconnected");
                    Ok(())
                },
            )
            .map_err(|err| err.into())
            .or_else(|err| match err {
                Error::Disconnect(DisconnectError::JustChecking) => Ok(()),
                err => Err(err),
            })
    }

    pub fn set_last_bmm_commitment_main_block_height(&self, height: usize) -> Result<(), Error> {
        self.values.insert(
            LAST_BMM_COMMITMENT_MAIN_BLOCK_HEIGHT,
            &(height as u64).to_be_bytes(),
        )?;
        Ok(())
    }

    pub fn get_last_bmm_commitment_main_block_height(&self) -> Result<Option<usize>, Error> {
        Ok(self
            .values
            .get(LAST_BMM_COMMITMENT_MAIN_BLOCK_HEIGHT)?
            .map(|height| BigEndian::read_u64(&height) as usize))
    }

    pub fn _update_deposits(&mut self, deposits: &[DepositUpdate]) -> Result<(), Error> {
        let tx = self.conn.transaction()?;
        trace!("updating deposits db with {} new deposits", deposits.len());
        // Get the last deposit stored in the db.
        let last_deposit = DB::_get_last_deposit(&tx)?;
        // Find the deposit that spends it.
        let deposits: Vec<&DepositUpdate> = deposits
            .iter()
            .skip_while(|deposit| match &last_deposit {
                Some(other) => !deposit.spends(other),
                // If there are no deposits in db we don't skip any
                // deposits.
                None => false,
            })
            .collect();

        // Apply all new deposits to deposit_balance table.
        let mut balances = HashMap::<String, Amount>::new();
        let mut prev_amount = last_deposit
            .map(|deposit| deposit.amount)
            .unwrap_or(Amount::ZERO);
        for deposit in deposits {
            if deposit.amount < prev_amount {
                continue;
            }
            trace!(
                "added {} to {}",
                deposit.amount - prev_amount,
                deposit.strdest
            );
            let balance = balances
                .entry(deposit.strdest.clone())
                .or_insert(Amount::ZERO);
            *balance += deposit.amount - prev_amount;
            prev_amount = deposit.amount;
        }
        for (address, delta) in balances {
            tx.execute(
                "INSERT INTO deposit_balance (address, delta)
                VALUES (?1, ?2)
                ON CONFLICT (address) DO
                UPDATE SET delta=delta+?2",
                (address, delta.to_sat()),
            )?;
        }
        tx.commit()?;
        // Set new last_deposit.
        Ok(())
    }

    fn _push_deposit(tx: &rusqlite::Transaction, deposit: &DepositUpdate) -> Result<(), Error> {
        tx.execute("DELETE FROM last_ctip", ())?;
        tx.execute(
            "INSERT INTO deposit (blockhash, txid, vout, amount, strdest) VALUES (?, ?, ?, ?, ?)",
            (
                deposit.blockhash.into_inner(),
                deposit.ctip.txid.into_inner(),
                deposit.ctip.vout,
                deposit.amount.to_sat(),
                deposit.strdest.clone(),
            ),
        )?;
        Ok(())
    }

    fn _get_last_deposit(tx: &rusqlite::Transaction) -> Result<Option<DepositUpdate>, Error> {
        let mut deposit = tx.prepare(
            "SELECT id, blockhash, txid, vout, amount, strdest FROM deposit ORDER BY id",
        )?;
        let deposit = deposit
            .query_row([], |row| {
                let deposit = DepositUpdate {
                    index: row.get(0)?,
                    blockhash: BlockHash::from_inner(row.get(1)?),
                    ctip: OutPoint {
                        txid: Txid::from_inner(row.get(2)?),
                        vout: row.get(3)?,
                    },
                    amount: Amount::from_sat(row.get(4)?),
                    strdest: row.get(5)?,
                };
                Ok(deposit)
            })
            .optional()?;
        Ok(deposit)
    }

    pub fn update_deposits(&self, deposits: &[MainDeposit]) -> Result<(), Error> {
        trace!("updating deposits db with {} new deposits", deposits.len());
        // New deposits are sorted in CTIP order.
        let sorted_deposits = DB::sort_deposits(deposits);
        // We get the last deposit stored in the db.
        let (last_index, last_deposit) = match self.get_last_deposit()? {
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
            deposits_batch.insert(&(index as u32).to_be_bytes(), bincode::serialize(&deposit)?);
            if deposit.amount() < prev_amount {
                continue;
            }
            trace!(
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
                        Some(balance) => Some(bincode::deserialize::<(u64, u64)>(&balance).or_else(abort)?),
                        None => None,
                    };
                    let balance = match balance {
                        Some(balance) => (balance.0, balance.1 + main_amount.to_sat()),
                        None => (0, main_amount.to_sat()),
                    };
                    let balance = bincode::serialize(&balance).or_else(abort)?;
                    deposit_balances.insert(address.as_bytes(), balance)?;
                    unbalanced_deposits.insert(address.as_bytes(), &[])?;
                }
                trace!("deposits updated successfuly");
                Ok(())
            })
            .map_err(|err| err.into())
    }

    pub fn sort_deposits(deposits: &[MainDeposit]) -> Vec<MainDeposit> {
        if deposits.is_empty() {
            return vec![];
        }
        let mut outpoint_to_deposit = HashMap::<OutPoint, MainDeposit>::new();
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
        let mut sorted_deposits = Vec::<MainDeposit>::new();
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

    pub fn get_last_deposit(&self) -> Result<Option<(usize, MainDeposit)>, Error> {
        self.deposits
            .last()?
            .map(|(index, deposit)| {
                Ok((
                    BigEndian::read_u32(index.as_ref()) as usize,
                    bincode::deserialize(deposit.as_ref())?,
                ))
            })
            .transpose()
    }

    pub fn remove_last_deposit(&self) -> Result<(), Error> {
        self.deposits.pop_max()?;
        Ok(())
    }

    pub fn is_outpoint_spent(&self, outpoint: &[u8]) -> Result<bool, Error> {
        self.spent_outpoints
            .contains_key(outpoint)
            .map_err(|err| err.into())
    }

    pub fn vote_bundle(&mut self, txid: &Txid) -> Result<(), Error> {
        trace!(
            "bundle {} is being marked as \"being voted on\" in db",
            txid
        );
        let inputs = self.get_inputs(txid)?;
        (
            &self.outpoint_to_withdrawal,
            &self.unspent_outpoints,
            &self.spent_outpoints,
        )
            .transaction(
                |(outpoint_to_withdrawal, unspent_outpoints, spent_outpoints)| {
                    for input in inputs.iter() {
                        // FIXME: Is this unwrap ok?
                        let withdrawal =
                            outpoint_to_withdrawal.get(bincode::serialize(input).unwrap())?;
                        if withdrawal.is_none() {
                            return abort(Error::NoWithdrawalInDb(input.hex()));
                        }
                        unspent_outpoints.remove(input.bytes())?;
                        spent_outpoints.insert(input.bytes(), &[])?;
                    }
                    Ok(())
                },
            )?;
        Ok(())
    }

    pub fn spend_bundle(&mut self, txid: &Txid) -> Result<(), Error> {
        trace!("bundle {} is being marked as \"spent\" in db", txid);
        let inputs = self.get_inputs(txid)?;
        (
            &self.spent_bundle_hashes,
            &self.unspent_outpoints,
            &self.spent_outpoints,
        )
            .transaction(
                |(spent_bundle_hashes, unspent_outpoints, spent_outpoints)| {
                    spent_bundle_hashes.insert(txid.as_inner(), &[])?;
                    for input in inputs.iter() {
                        unspent_outpoints.remove(input.bytes())?;
                        spent_outpoints.insert(input.bytes(), &[])?;
                    }
                    Ok(())
                },
            )
            .map_err(|err: TransactionError<Error>| err.into())
    }

    pub fn fail_bundle(&mut self, txid: &Txid) -> Result<(), Error> {
        trace!("bundle {} is being marked as \"failed\" in db", txid);
        let inputs = self.get_inputs(txid)?;
        (
            &self.failed_bundle_hashes,
            &self.unspent_outpoints,
            &self.spent_outpoints,
            &self.values,
        )
            .transaction(
                |(failed_bundle_hashes, unspent_outpoints, spent_outpoints, values)| {
                    failed_bundle_hashes.insert(txid.as_inner(), &[])?;
                    for input in inputs.iter() {
                        spent_outpoints.remove(input.bytes())?;
                        unspent_outpoints.insert(input.bytes(), &[])?;
                    }
                    let last_failed_bundle_height = match values.get(SIDE_BLOCK_HEIGHT)? {
                        Some(height) => height,
                        None => return abort(Error::NoBlockHeight),
                    };
                    values.insert(LAST_FAILED_BUNDLE_HEIGHT, last_failed_bundle_height)?;
                    Ok(())
                },
            )?;
        Ok(())
    }

    pub fn get_blocks_since_last_failed_bundle(&self) -> Result<usize, Error> {
        let block_height = match self.values.get(SIDE_BLOCK_HEIGHT)? {
            Some(block_height) => BigEndian::read_u64(&block_height) as usize,
            // No block height set means we are at genesis block.
            None => 0,
        };
        let last_failed_bundle_height = match self.values.get(LAST_FAILED_BUNDLE_HEIGHT)? {
            Some(last_failed_bundle_height) => {
                BigEndian::read_u64(&last_failed_bundle_height) as usize
            }
            // If there were no bundles then last bundle height is 0.
            None => 0,
        };
        Ok(block_height - last_failed_bundle_height)
    }

    pub fn get_spent_bundle_hashes(&self) -> Result<HashSet<Txid>, Error> {
        self.spent_bundle_hashes
            .iter()
            .map(|item| {
                let (txid, _) = item?;
                let mut txid_inner: [u8; 32] = Default::default();
                txid_inner.copy_from_slice(&txid);
                Ok(Txid::from_inner(txid_inner))
            })
            .collect()
    }

    pub fn get_failed_bundle_hashes(&self) -> Result<HashSet<Txid>, Error> {
        self.failed_bundle_hashes
            .iter()
            .map(|item| {
                let (txid, _) = item?;
                let mut txid_inner: [u8; 32] = Default::default();
                txid_inner.copy_from_slice(&txid);
                Ok(Txid::from_inner(txid_inner))
            })
            .collect()
    }

    pub fn get_inputs(&mut self, txid: &Txid) -> Result<Vec<Outpoint>, Error> {
        let hash = txid.into_inner();
        let inputs = match self.bundle_hash_to_inputs.get(hash)? {
            Some(inputs) => inputs,
            None => return Ok(vec![]),
        };
        bincode::deserialize::<Vec<Outpoint>>(&inputs).map_err(|err| err.into())
    }

    pub fn get_unspent_withdrawals(&self) -> Result<HashMap<Outpoint, Withdrawal>, Error> {
        self.unspent_outpoints
            .iter()
            .map(|item| {
                let (outpoint, _) = item?;
                let withdrawal = match self.outpoint_to_withdrawal.get(&outpoint)? {
                    Some(withdrawal) => withdrawal,
                    None => return Err(Error::NoWithdrawalInDb(hex::encode(&outpoint))),
                };
                let withdrawal = bincode::deserialize::<Withdrawal>(&withdrawal)?;
                Ok((Outpoint(outpoint.to_vec()), withdrawal))
            })
            .collect()
    }

    // TODO: Investigate possibility of determining mainchain fee automatically.
    pub fn create_bundle(&mut self) -> Result<Option<bitcoin::Transaction>, Error> {
        // Weight of a bundle with 0 outputs.
        const BUNDLE_0_WEIGHT: usize = 332;
        // Weight of a single output.
        const OUTPUT_WEIGHT: usize = 128;
        // Turns out to be 3122.
        const MAX_BUNDLE_OUTPUTS: usize =
            (bitcoin::policy::MAX_STANDARD_TX_WEIGHT as usize - BUNDLE_0_WEIGHT) / OUTPUT_WEIGHT;
        // Maximum possible weight for a bundle turns out to be 399948 weight
        // units, just 52 units short of MAX_STANDARD_TX_WEIGHT = 400000.

        trace!("creating a new bundle from unspent withdrawals in db",);
        let withdrawals = self.unspent_outpoints.iter().map(|item| {
            let (outpoint, _) = item?;
            let withdrawal = match self.outpoint_to_withdrawal.get(&outpoint)? {
                Some(withdrawal) => withdrawal,
                None => return Err(Error::NoWithdrawalInDb(hex::encode(&outpoint))),
            };
            let withdrawal = bincode::deserialize::<Withdrawal>(&withdrawal)?;
            Ok((outpoint.to_vec(), withdrawal))
        });

        // Aggregate all outputs by destination.
        // destination -> (amount, mainchain fee, height)
        let mut dest_to_withdrawal = HashMap::<[u8; 20], (u64, u64, u64)>::new();
        let mut outpoints = vec![];
        for withdrawal in withdrawals {
            let (outpoint, output) = withdrawal?;
            outpoints.push(outpoint);
            let (amount, mainchain_fee, height) =
                dest_to_withdrawal.entry(output.dest).or_insert((0, 0, 0));
            // Add up all amounts.
            *amount += output.amount;
            // Set the newest mainchain fee.
            if *height < output.height {
                *mainchain_fee = output.mainchain_fee;
            }
            // Height is only used for sorting so at this point it doesn't
            // matter, but we set it to the lowest one anyway just to have a
            // reasonable value for Withdrawal::height later.
            *height = std::cmp::min(*height, output.height);
        }
        trace!(
            "{} unspent withdrawals were aggregated into {} bundle outputs",
            outpoints.len(),
            dest_to_withdrawal.len()
        );
        // We iterate over our HashMap with aggregated (amount, fee, height)
        // tripples and convert it into a vector of Withdrawals.
        let mut outputs: Vec<Withdrawal> = dest_to_withdrawal
            .into_iter()
            .map(|(dest, (amount, mainchain_fee, height))| Withdrawal {
                dest,
                amount,
                mainchain_fee,
                height,
            })
            .collect();
        // Don't create an empty bundle.
        if outputs.is_empty() {
            trace!("there are no unspent withdrawals in db so we don't create a new bundle");
            return Ok(None);
        }
        // Sort the outputs in descending order from best to worst.
        //
        // See documentation for Ord trait implemenation of Withdrawal for
        // explanation of how we compare outputs.
        outputs.sort_by_key(|a| std::cmp::Reverse(*a));
        let mut fee = 0;
        let mut bundle_outputs = vec![];
        for output in &outputs {
            if bundle_outputs.len() > MAX_BUNDLE_OUTPUTS {
                trace!("number of bundle outputs {} is greater than maximum number of bundle outputs {} skipping last {} outputs",
                       outputs.len(),
                       MAX_BUNDLE_OUTPUTS,
                       MAX_BUNDLE_OUTPUTS - outputs.len()
                );
                break;
            }
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
        trace!(
            "created a new bundle with fee = {} and {} outputs",
            fee,
            bundle_outputs.len()
        );
        let bundle = bitcoin::Transaction {
            version: 2,
            lock_time: bitcoin::PackedLockTime(0),
            input: vec![txin],
            output: [vec![return_dest_txout, mainchain_fee_txout], bundle_outputs].concat(),
        };
        if bundle.weight() > bitcoin::policy::MAX_STANDARD_TX_WEIGHT as usize {
            error!(
                "bundle weight {} is greater than maximum transaction weight {}",
                bundle.weight(),
                bitcoin::policy::MAX_STANDARD_TX_WEIGHT
            );
            return Err(Error::BundleTooHeavy(bundle.weight()));
        }
        {
            let hash = bundle.txid();
            let hash = hash.into_inner();
            let inputs = outpoints;
            let inputs = bincode::serialize(&inputs)?;
            self.bundle_hash_to_inputs.insert(hash, inputs)?;
        }
        trace!("bundle was created successfuly");
        Ok(Some(bundle))
    }

    pub fn flush(&mut self) -> Result<usize, Error> {
        self.db.flush().map_err(|err| err.into())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("there are more than one ctip in last_ctip table")]
    MultipleCtips,
    #[error("rusqlite error")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("sled error")]
    Sled(#[from] sled::Error),
    #[error("bincode error")]
    Bincode(#[from] bincode::Error),
    #[error("connect block error")]
    Connect(#[from] ConnectError),
    #[error("disconnect block error")]
    Disconnect(#[from] DisconnectError),
    #[error("no deposit for {0}")]
    NoDeposit(String),
    #[error("no current block height stored in db")]
    NoBlockHeight,
    #[error("withdrawal for outpoint {0} is not in db")]
    NoWithdrawalInDb(String),
    #[error("withdrawal bundle too heavy, weight = {0}")]
    BundleTooHeavy(usize),
    #[error("from utf8 error")]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error("error converting slice to fixed size array")]
    TryFromSlice(#[from] std::array::TryFromSliceError),
}

impl<E: Into<Error>> From<TransactionError<E>> for Error {
    fn from(error: TransactionError<E>) -> Error {
        match error {
            TransactionError::Abort(err) => err.into(),
            TransactionError::Storage(err) => err.into(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectError {
    #[error("deposit is unbalanced {address}: {side_balance} out of {main_balance}")]
    Unbalanced {
        address: String,
        side_balance: bitcoin::Amount,
        main_balance: bitcoin::Amount,
    },
    #[error("wrong refund amount: outpoint: {outpoint}, actual amount: {actual_amount}, refunded amount: {refunded_amount}")]
    WrongRefundAmount {
        outpoint: String,
        actual_amount: u64,
        refunded_amount: u64,
    },
    #[error("can't refund already spent outpoint: {outpoint}")]
    RefundedSpentOutpoint { outpoint: String },
    #[error("there is no deposit with address {0}")]
    NoAddress(String),
    #[error("just checking")]
    JustChecking,
}

#[derive(thiserror::Error, Debug)]
pub enum DisconnectError {
    #[error("deposit to address {0} doesn't exist in db")]
    NoDepositInDB(String),
    #[error("just checking")]
    JustChecking,
    #[error("cannot disconnect genesis block")]
    Genesis,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{prelude::SliceRandom, Rng, SeedableRng};

    // Split a number into a vector of random terms.
    struct SplitTerms(u64);
    impl Dummy<SplitTerms> for Vec<u64> {
        fn dummy_with_rng<R: Rng + ?Sized>(number: &SplitTerms, rng: &mut R) -> Vec<u64> {
            let SplitTerms(mut number) = *number;
            let mut terms = vec![];
            while number > 0 {
                let term = rng.gen_range(1..=number);
                number -= term;
                terms.push(term);
            }
            terms
        }
    }

    #[quickcheck]
    fn terms_sum_to_number(number: u64) -> bool {
        let terms = SplitTerms(number).fake::<Vec<u64>>();
        terms.iter().sum::<u64>() == number
    }

    #[derive(Debug, Clone)]
    struct Balances(HashMap<String, u64>);

    impl quickcheck::Arbitrary for Balances {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            g.size().fake::<Balances>()
        }
    }

    impl Dummy<usize> for Balances {
        // Generates a random Address -> Amount balances HashMap.
        fn dummy_with_rng<R: Rng + ?Sized>(size: &usize, rng: &mut R) -> Balances {
            const ADDRESS_LENGTH: usize = 10;
            let max_amount: u64 = Amount::MAX_MONEY.to_sat() / (*size as u64);
            let balances: HashMap<String, u64> = (0..*size)
                .map(|_| {
                    let address = ADDRESS_LENGTH.fake::<String>();
                    let balance = (0..max_amount).fake::<u64>();
                    (address, balance)
                })
                .collect();
            Balances(balances)
        }
    }

    impl Dummy<Balances> for HashMap<String, Vec<u64>> {
        // Split balances into vectors of random terms.
        fn dummy_with_rng<R: Rng + ?Sized>(
            balances: &Balances,
            rng: &mut R,
        ) -> HashMap<String, Vec<u64>> {
            let Balances(balances) = balances;
            let balances = (*balances).clone();
            balances
                .into_iter()
                .map(|(address, balance)| (address, SplitTerms(balance).fake()))
                .collect()
        }
    }

    #[quickcheck]
    fn balance_terms_sum_to_balances(balances: HashMap<String, u64>) -> bool {
        let split_balances = Balances(balances.clone()).fake::<HashMap<String, Vec<u64>>>();
        let summed_balances = split_balances
            .into_iter()
            .map(|(address, terms)| (address, terms.iter().sum::<u64>()))
            .collect();
        balances == summed_balances
    }

    impl Dummy<Balances> for Vec<(String, u64)> {
        // Convert balances into a shuffled sequence of (address, delta) pairs.
        fn dummy_with_rng<R: Rng + ?Sized>(balances: &Balances, rng: &mut R) -> Vec<(String, u64)> {
            let balances_split_into_terms: HashMap<String, Vec<u64>> = balances.fake();
            let mut address_delta_pairs: Vec<(String, u64)> = balances_split_into_terms
                .into_iter()
                .map(|(address, deltas)| std::iter::repeat(address).zip(deltas.into_iter()))
                .flatten()
                .collect();
            address_delta_pairs.shuffle(rng);
            address_delta_pairs
        }
    }

    fn convert_to_totals(address_delta_pairs: Vec<(String, u64)>) -> Vec<(String, u64)> {
        let mut address_total_pairs = vec![];
        let mut total = 0;
        for (address, delta) in address_delta_pairs {
            total += delta;
            address_total_pairs.push((address, total));
        }
        address_total_pairs
    }

    impl Dummy<Balances> for Vec<DepositUpdate> {
        // Convert balances into a shuffled sequence of (address, delta) pairs.
        fn dummy_with_rng<R: Rng + ?Sized>(balances: &Balances, rng: &mut R) -> Vec<DepositUpdate> {
            let address_delta_pairs = balances.fake::<Vec<(String, u64)>>();
            let address_total_pairs = convert_to_totals(address_delta_pairs);
            let deposit_updates: Vec<DepositUpdate> = address_total_pairs
                .iter()
                .enumerate()
                .map(|(index, (address, total))| {
                    const MAX_VOUT: u32 = 100;
                    let blockhash: [u8; 32] = Faker.fake();
                    let blockhash = BlockHash::from_inner(blockhash);
                    let txid: [u8; 32] = Faker.fake();
                    let txid = Txid::from_inner(txid);
                    let vout: u32 = (0..MAX_VOUT).fake();
                    let ctip = OutPoint { txid, vout };
                    DepositUpdate {
                        index,
                        blockhash,
                        ctip,
                        strdest: address.clone(),
                        amount: Amount::from_sat(*total),
                    }
                })
                .collect();
            deposit_updates
        }
    }

    #[derive(Clone, Debug)]
    pub struct DepositUpdates(pub Vec<DepositUpdate>);

    pub struct DepositsFaker {
        num_deposits: usize,
        max_vout: usize,
        balances: HashMap<String, u64>,
    }

    // impl Dummy<DepositsFaker> for DepositUpdates {
    //     fn dummy_with_rng<R: Rng + ?Sized>(config: &DepositsFaker, rng: &mut R) -> DepositUpdates {}
    // }

    impl Dummy<Faker> for DepositUpdates {
        fn dummy_with_rng<R: Rng + ?Sized>(_: &Faker, rng: &mut R) -> DepositUpdates {
            const NUM_ADDRESSES: usize = 10;
            const NUM_DEPOSITS: usize = 10000;
            const MAX_VOUT: u32 = 100;
            let max_amount: u64 = Amount::MAX_MONEY.to_sat() / (NUM_DEPOSITS as u64);
            let mut amount = Amount::ZERO;

            let addresses: Vec<String> =
                (0..NUM_ADDRESSES).map(|_| Faker.fake::<String>()).collect();
            let deposit_updates = (0..NUM_DEPOSITS)
                .map(|index| {
                    let blockhash: [u8; 32] = Faker.fake();
                    let blockhash = BlockHash::from_inner(blockhash);
                    let txid: [u8; 32] = Faker.fake();
                    let txid = Txid::from_inner(txid);
                    let vout: u32 = (0..MAX_VOUT).fake();
                    let delta: u64 = (0..max_amount).fake();
                    let delta = Amount::from_sat(delta);
                    amount += delta;
                    let strdest: String = addresses.choose(rng).unwrap().clone();

                    DepositUpdate {
                        index,
                        blockhash,
                        ctip: OutPoint { txid, vout },
                        amount,
                        strdest,
                    }
                })
                .collect();
            DepositUpdates(deposit_updates)
        }
    }

    #[quickcheck]
    fn deposits_work(balances: Balances) -> bool {
        let mut db = DB::new("/tmp/drivechain").unwrap();
        let deposit_updates = balances.fake::<Vec<DepositUpdate>>();
        db._update_deposits(&deposit_updates).unwrap();
        let outputs = db._get_deposit_outputs().unwrap();
        balances.0 == outputs
    }
}
