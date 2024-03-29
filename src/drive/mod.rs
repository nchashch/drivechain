mod client;
mod db;
mod deposit;
mod withdrawal;
use bitcoin::hash_types::{BlockHash, ScriptHash, Txid};
use bitcoin::hashes::{sha256, Hash};
use bitcoin::Amount;
pub use deposit::Deposit;
use log::{info, trace};
use std::collections::HashMap;
pub use withdrawal::Withdrawal;

/// An object that provides a standard API for implementing a sidechain.
///
/// It holds an in memory BMM cache, a handle to an RPC client
/// connected to mainchain, and a handle to a sled database persisting all
/// sidechain state in a standard format.
pub struct Drivechain {
    client: client::Client,
    bmm_cache: BMMCache,
    db: db::DB,
}

// TODO: Create a list of errors with error codes and explanations.
// TODO: Export error code and explanation in FFIs.
/// An error type returned by Drivechain struct's methods in case of failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Database error.
    #[error("db error")]
    Db(#[from] db::Error),
    /// Mainchain RPC client error.
    #[error("client error")]
    Client(#[from] client::Error),
    /// Wrong withdrawal mainchain address type (it must be a P2SH address).
    #[error("wrong mainchain address type")]
    WrongMainAddressType,
    /// Failed to convert 20 mainchain bytes into a ScriptHash.
    #[error("hash error")]
    HashError(#[from] bitcoin::hashes::Error),
}

impl Drivechain {
    /// Create a new Drivechain object.
    ///
    /// * `db_path` - path to directory where the sled database will be stored.
    /// * `this_sidechain` - sidechain number.
    /// * `host` - hostname used for mainchain RPC (most likely "localhost").
    /// * `port` - port used for mainchain RPC.
    /// * `rpcuser` - mainchain RPC user.
    /// * `rpcpassword` - mainchain RPC password.
    pub fn new<P: AsRef<std::path::Path> + std::fmt::Display>(
        db_path: P,
        this_sidechain: usize,
        host: &str,
        port: u16,
        rpcuser: &str,
        rpcpassword: &str,
    ) -> Result<Drivechain, Error> {
        env_logger::init();
        trace!("creating drivechain object");

        let client = client::Client {
            this_sidechain,
            host: host.to_string(),
            port,
            rpcuser: rpcuser.to_string(),
            rpcpassword: rpcpassword.to_string(),
        };

        let mut bmm_cache = BMMCache::new();
        let mainchain_tip_hash = client.get_mainchain_tip()?;
        bmm_cache.prev_main_block_hash = Some(mainchain_tip_hash);
        trace!("drivechain object created successfuly");
        Ok(Drivechain {
            client,
            bmm_cache,
            db: db::DB::new(db_path)?,
        })
    }

    /// This is used to get `prev_main_block_hash` the for a sidechain block
    /// header. Mainchain tip will become the previous block when the sidechain
    /// block is BMMed.
    pub fn get_mainchain_tip(&self) -> Result<BlockHash, Error> {
        self.client.get_mainchain_tip().map_err(|err| err.into())
    }

    /// This is called by sidechain "miners" to get a sidechain block BMMed.
    pub fn attempt_bmm(
        &mut self,
        side_block_hash: &BlockHash,
        prev_main_block_hash: &BlockHash,
        amount: Amount,
    ) -> Result<(), Error> {
        self.attempt_bundle_broadcast()?;
        trace!(
            "attempting to create a bmm request for block with hash = {} and with bribe = {}",
            side_block_hash,
            amount
        );
        // Create a BMM request.
        let txid =
            self.client
                .send_bmm_request(side_block_hash, prev_main_block_hash, 0, amount)?;
        let bmm_request = BMMRequest {
            txid,
            prev_main_block_hash: *prev_main_block_hash,
            side_block_hash: *side_block_hash,
        };
        // and add request data to the requests vec.
        self.bmm_cache.requests.push(bmm_request);
        self.bmm_cache.prev_main_block_hash = Some(*prev_main_block_hash);
        trace!("bmm request was created successfuly txid = {}", txid);
        Ok(())
    }

    /// Check the status of last BMM request (the one created by `attempt_bmm`).
    pub fn confirm_bmm(&mut self) -> Result<BMMState, Error> {
        let mainchain_tip_hash = self.client.get_mainchain_tip()?;
        trace!(
            "attempting to confirm that a block was bmmed at mainchain tip = {}",
            &mainchain_tip_hash
        );
        if self.bmm_cache.prev_main_block_hash == Some(mainchain_tip_hash) {
            trace!("no new blocks on mainchain so sidechain block wasn't bmmed");
            // If no blocks were mined on mainchain no bmm requests could have
            // possibly been accepted.
            return Ok(BMMState::Pending);
        }
        // Mainchain tip has changed so all requests for previous tip are now
        // delete all requests with drain method.
        trace!("new blocks on mainchain, checking if sidechain block was bmmed");
        for request in self.bmm_cache.requests.drain(..) {
            trace!(
                "checking bmm request for side:block hash = {}",
                request.side_block_hash
            );
            // We check if our request was included in a mainchain block.
            if let Some(main_block_hash) = self.client.get_tx_block_hash(&request.txid)? {
                trace!("bmm commitment was included in mainchain block");
                if self.client.get_prev_block_hash(&main_block_hash)?
                    != request.prev_main_block_hash
                {
                    trace!("bmm commitment is invalid because the mainchain block doesn't follow previous mainchcain block");
                    continue;
                }
                // And we check that side_block_hash was actually included in
                // coinbase on mainchain.
                if let Ok(verified) = self
                    .client
                    .verify_bmm(&main_block_hash, &request.side_block_hash)
                {
                    trace!("bmm request was successful");
                    info!(
                        "sidechain block {} was successfuly bmmed in mainchain block {} at {}",
                        &request.side_block_hash,
                        &main_block_hash,
                        chrono::DateTime::<chrono::Utc>::from(
                            std::time::UNIX_EPOCH
                                + std::time::Duration::from_secs(verified.time as u64)
                        ),
                    );
                    return Ok(BMMState::Succeded);
                }
            }
        }
        trace!("no sidechain block was bmmed");
        Ok(BMMState::Failed)
    }

    /// Format `str_dest` with the proper `s{sidechain_number}_` prefix and a
    /// checksum postfix for calling createsidechaindeposit on mainchain.
    pub fn format_deposit_address(&self, str_dest: &str) -> String {
        let deposit_address: String = format!("s{}_{}_", self.client.this_sidechain, str_dest);
        let hash = sha256::Hash::hash(deposit_address.as_bytes()).to_string();
        let hash: String = hash[..6].into();
        format!("{}{}", deposit_address, hash)
    }

    /// Format a 20 byte hash into a human readable mainchain address.
    pub fn format_mainchain_address(dest: [u8; 20]) -> Result<String, Error> {
        let script_hash = ScriptHash::from_slice(&dest)?;
        let address = bitcoin::Address {
            payload: bitcoin::util::address::Payload::ScriptHash(script_hash),
            // FIXME: Don't hardcode this.
            network: bitcoin::network::constants::Network::Regtest,
        };
        Ok(address.to_string())
    }

    fn update_deposits(&self, height: Option<usize>) -> Result<(), Error> {
        let mut last_deposit = self
            .db
            .get_last_deposit()?
            .map(|(_, last_deposit)| last_deposit);
        trace!("updating deposits, last known deposit = {:?}", last_deposit);
        while !last_deposit.clone().map_or(true, |last_deposit| {
            self.client.verify_deposit(&last_deposit).unwrap_or(false)
        }) {
            trace!("removing invalid last deposit = {:?}", last_deposit);
            self.db.remove_last_deposit()?;
            last_deposit = self
                .db
                .get_last_deposit()?
                .map(|(_, last_deposit)| last_deposit);
        }
        let last_output = last_deposit.map(|deposit| (deposit.tx.txid(), deposit.nburnindex));
        let deposits = self.client.get_deposits(last_output)?;
        let deposits = match height {
            Some(height) => {
                // FIXME: Add height field to listsidechaindeposits mainchain
                // RPC to get rid of this code.
                let heights: HashMap<BlockHash, usize> = deposits
                    .iter()
                    .map(|deposit| {
                        Ok((
                            deposit.blockhash,
                            self.client.get_main_block_height(&deposit.blockhash)?,
                        ))
                    })
                    .collect::<Result<HashMap<BlockHash, usize>, Error>>()?;
                deposits
                    .into_iter()
                    .filter(|deposit| heights[&deposit.blockhash] < height)
                    .collect()
            }
            None => deposits,
        };
        self.db.update_deposits(deposits.as_slice())?;
        let last_deposit = self
            .db
            .get_last_deposit()?
            .map(|(_, last_deposit)| last_deposit);
        trace!(
            "deposits were updated, new last known deposit = {:?}",
            last_deposit
        );
        Ok(())
    }

    fn update_bundles(&mut self) -> Result<(), Error> {
        trace!("updating bundle statuses");
        let known_failed = self.db.get_failed_bundle_hashes()?;
        let failed = self.client.get_failed_withdrawal_bundle_hashes()?;
        let failed = failed.difference(&known_failed);
        for txid in failed {
            trace!("bundle {} failed", txid);
            self.db.fail_bundle(txid)?;
        }
        let known_spent = self.db.get_spent_bundle_hashes()?;
        let spent = self.client.get_spent_withdrawal_bundle_hashes()?;
        let spent = spent.difference(&known_spent);
        for txid in spent {
            trace!("bundle {} is spent", txid);
            self.db.spend_bundle(txid)?;
        }
        let voting = self.client.get_voting_withdrawal_bundle_hashes()?;
        for txid in voting {
            trace!("bundle {} is being voted on", txid);
            self.db.vote_bundle(&txid)?;
        }
        trace!("bundle statuses were updated successfuly");
        Ok(())
    }

    // TODO: Raise alarm if bundle hash being voted on is wrong.
    fn attempt_bundle_broadcast(&mut self) -> Result<(), Error> {
        trace!("attempting to create and broadcast a new bundle");
        self.update_bundles()?;
        // Wait for some time after a failed bundle to give people an
        // opportunity to refund. If we would create a new bundle immediately,
        // some outputs would be included in it immediately again, and so they
        // would never become refundable.
        //
        // FIXME: Make this value different for regtest/testnet/mainnet.
        const BUNDLE_WAIT_PERIOD: usize = 5;
        let blocks_since_last_failed_bundle = self.db.get_blocks_since_last_failed_bundle()?;
        if blocks_since_last_failed_bundle < BUNDLE_WAIT_PERIOD {
            info!("cannot create new bundle, because last faild bundle was too soon, need to wait {} more blocks", BUNDLE_WAIT_PERIOD - blocks_since_last_failed_bundle);
            // FIXME: Figure out what type this should actually be. Just
            // returning an Ok(()) seems wrong.
            return Ok(());
        }
        let voting = self.client.get_voting_withdrawal_bundle_hashes()?;
        // If a bundle is already being voted on we don't need to broadcast a
        // new one.
        if !voting.is_empty() {
            info!(
                "cannot create new bundle, there is already a bundle being voted on: {:?}",
                voting
            );
            // FIXME: Figure out what type this should actually be. Just
            // returning an Ok(()) seems wrong.
            return Ok(());
        }
        let bundle = match self.db.create_bundle()? {
            Some(bundle) => bundle,
            // FIXME: Figure out what type this should actually be. Just
            // returning an Ok(()) seems wrong.
            None => {
                info!("cannot create new bundle, there are no unspent withdrawals");
                return Ok(());
            }
        };
        let status = self.get_bundle_status(&bundle.txid())?;
        info!("bundle {} created, it is {}", bundle.txid(), status,);
        trace!("bundle = {:?}", bundle);
        // We broadcast a bundle only if it was not seen before, meaning it is
        // neither failed nor spent.
        if status == BundleStatus::New {
            self.client.broadcast_withdrawal_bundle(&bundle)?;
            self.db.vote_bundle(&bundle.txid())?;
            info!("bundle is new, so it is broadcast to mainchain");
        } else {
            info!("bundle is {}, so it is ignored", status);
        }
        Ok(())
    }

    fn get_bundle_status(&self, txid: &Txid) -> Result<BundleStatus, Error> {
        let voting = self.client.get_voting_withdrawal_bundle_hashes()?;
        let failed = self.client.get_failed_withdrawal_bundle_hashes()?;
        let spent = self.client.get_spent_withdrawal_bundle_hashes()?;
        Ok(if voting.contains(txid) {
            BundleStatus::Voting
        } else if failed.contains(txid) {
            BundleStatus::Failed
        } else if spent.contains(txid) {
            BundleStatus::Spent
        } else {
            BundleStatus::New
        })
    }

    /// This must be called when a sidechain block becomes part of consensus
    /// (most likely when it was successfuly BMMed).
    pub fn connect_block(
        &mut self,
        deposits: &[Deposit],
        withdrawals: &HashMap<Vec<u8>, withdrawal::Withdrawal>,
        refunds: &HashMap<Vec<u8>, u64>,
        just_check: bool,
    ) -> Result<(), Error> {
        let height = self.db.get_last_bmm_commitment_main_block_height()?;
        self.update_deposits(height)?;
        self.db
            .connect_block(deposits, withdrawals, refunds, just_check)?;
        Ok(())
    }

    /// This must be called on a block when a sidechain it is no longer part of
    /// consensus (most likely because of a mainchain reorg).
    pub fn disconnect_block(
        &mut self,
        deposits: &[Deposit],
        withdrawals: &[Vec<u8>],
        refunds: &[Vec<u8>],
        just_check: bool,
    ) -> Result<(), Error> {
        self.db
            .disconnect_block(deposits, withdrawals, refunds, just_check)?;
        Ok(())
    }

    /// `prev_main_block_hash` must be included in a sidechain block header.
    pub fn verify_bmm(
        &self,
        prev_main_block_hash: &BlockHash,
        side_block_hash: &BlockHash,
    ) -> Result<bool, Error> {
        let main_block_hash = match self.client.get_next_main_block(prev_main_block_hash) {
            Ok(mbh) => mbh,
            Err(_) => return Ok(false),
        };
        if !self.is_main_block_connected(&main_block_hash)? {
            return Ok(false);
        }
        let verified = self
            .client
            .verify_bmm(&main_block_hash, side_block_hash)
            .is_ok();
        let height = self.client.get_main_block_height(&main_block_hash)?;
        let last_height = self
            .db
            .get_last_bmm_commitment_main_block_height()?
            .unwrap_or(0);
        if height > last_height {
            self.db.set_last_bmm_commitment_main_block_height(height)?;
        }
        Ok(verified)
    }

    /// This is used for dealing with mainchain reorgs.
    pub fn is_main_block_connected(&self, main_block_hash: &BlockHash) -> Result<bool, Error> {
        self.client
            .is_main_block_connected(main_block_hash)
            .map_err(|err| err.into())
    }

    // FIXME: Rename `outpoint` to `withdrawal`.
    /// Check if a withdrawal output can be spent (refunded) or not.
    pub fn is_outpoint_spent(&self, outpoint: &[u8]) -> Result<bool, Error> {
        self.db
            .is_outpoint_spent(outpoint)
            .map_err(|err| err.into())
    }

    /// Flush the drivechain database to disk.
    pub fn flush(&mut self) -> Result<usize, Error> {
        trace!("flushing the db");
        self.db.flush().map_err(|err| err.into())
    }

    /// Get deposits that must be paid out in the next block.
    pub fn get_deposit_outputs(&self) -> Result<Vec<Deposit>, Error> {
        self.update_deposits(None)?;
        self.db.get_deposit_outputs().map_err(|err| err.into())
    }

    /// Get withdrawals that are not spent and not being voted on.
    pub fn get_unspent_withdrawals(&self) -> Result<HashMap<Vec<u8>, Withdrawal>, Error> {
        self.db.get_unspent_withdrawals().map_err(|err| err.into())
    }

    /// The extracted 20 bytes are used in withdrawal outputs or withdrawal
    /// transactions in a sidechain.
    pub fn extract_mainchain_address_bytes(address: &bitcoin::Address) -> Result<[u8; 20], Error> {
        match address.payload {
            bitcoin::util::address::Payload::ScriptHash(bytes) => Ok(bytes.into_inner()),
            _ => Err(Error::WrongMainAddressType),
        }
    }

    // FIXME: Add a way to check network, so you cannot send mainnet funds to
    // testnet/regtest address.
    /// This is useful for implementing the "withdraw" RPC on a sidechain.
    pub fn get_new_mainchain_address(&self) -> Result<bitcoin::Address, Error> {
        self.client
            .get_new_mainchain_address()
            .map_err(|err| err.into())
    }

    // FIXME: Pass through actually usable error messages in case of RPC error.
    /// Call mainchain RPC to create a deposit.
    pub fn create_deposit(
        &self,
        address: &str,
        amount: Amount,
        fee: Amount,
    ) -> Result<Txid, Error> {
        let address = self.format_deposit_address(address);
        self.client
            .create_sidechain_deposit(&address, amount, fee)
            .map_err(|err| err.into())
    }

    /// Mine a mainchain block (only works in regtest mode).
    pub fn generate(&self, n: usize) -> Result<Vec<BlockHash>, Error> {
        self.client.generate(n).map_err(|err| err.into())
    }
}

#[derive(Debug)]
struct BMMCache {
    // TODO: Can this be an Option?
    requests: Vec<BMMRequest>,
    prev_main_block_hash: Option<BlockHash>,
}

impl BMMCache {
    fn new() -> BMMCache {
        BMMCache {
            requests: Vec::new(),
            prev_main_block_hash: None,
        }
    }
}

/// State of a BMM request.
#[derive(Debug)]
pub enum BMMState {
    /// A `Succeded` request was successfully commited in a mainchain block.
    Succeded,
    /// A `Failed` request failed to be commited in a mainchain block.
    Failed,
    /// A `Pending` request is waiting to be commited in a mainchain block.
    Pending,
}

#[derive(Debug)]
struct BMMRequest {
    txid: Txid,
    side_block_hash: BlockHash,
    prev_main_block_hash: BlockHash,
}

#[derive(Debug, Eq, PartialEq)]
enum BundleStatus {
    New,
    Voting,
    Failed,
    Spent,
}

impl std::fmt::Display for BundleStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BundleStatus::New => write!(f, "new"),
            BundleStatus::Voting => write!(f, "being voted on"),
            BundleStatus::Failed => write!(f, "failed"),
            BundleStatus::Spent => write!(f, "spent"),
        }
    }
}
