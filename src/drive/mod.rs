mod client;
mod coinbase_data;
mod db;
pub mod deposit;
pub mod withdrawal;
use bitcoin::hash_types::{BlockHash, TxMerkleNode, Txid};
use bitcoin::hashes::{sha256, Hash};
use bitcoin::util::amount::Amount;
use client::DrivechainClient;
pub use coinbase_data::CoinbaseData;
pub use deposit::Deposit;
use log::{info, trace};
pub use withdrawal::WithdrawalOutput;

#[derive(Debug)]
pub struct Block {
    pub data: Vec<u8>,
    pub time: i64,
    pub main_block_hash: BlockHash,
}

pub struct Drivechain {
    pub client: DrivechainClient,
    bmm_cache: BMMCache,
    pub db: db::DB,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("db error")]
    Db(#[from] db::Error),
    #[error("client error")]
    Client(#[from] client::Error),
    #[error("error report")]
    Report(#[from] color_eyre::Report),
}

impl Drivechain {
    pub fn new<P: AsRef<std::path::Path> + std::fmt::Display>(
        db_path: P,
        this_sidechain: usize,
        rpcuser: String,
        rpcpassword: String,
    ) -> Result<Drivechain, Error> {
        trace!("creating drivechain object");
        color_eyre::install()?;
        const LOCALHOST: &str = "127.0.0.1";
        const MAINCHAIN_PORT: usize = 18443;

        let client = DrivechainClient {
            this_sidechain,
            host: LOCALHOST.into(),
            port: MAINCHAIN_PORT,
            rpcuser,
            rpcpassword,
        };

        trace!("drivechain object created successfuly");
        Ok(Drivechain {
            client,
            bmm_cache: BMMCache::new(),
            db: db::DB::new(db_path)?,
        })
    }

    pub fn get_coinbase_data(
        &self,
        prev_side_block_hash: BlockHash,
    ) -> Result<CoinbaseData, Error> {
        let prev_main_block_hash = self.client.get_mainchain_tip()?;
        trace!(
            "getting coinbase data for prev side block hash = {} and prev main block hash = {}",
            &prev_side_block_hash,
            &prev_main_block_hash
        );
        Ok(CoinbaseData {
            prev_main_block_hash,
            prev_side_block_hash,
        })
    }

    // Attempts to blind merge mine a block.
    pub fn attempt_bmm(
        &mut self,
        critical_hash: &TxMerkleNode,
        block_data: &[u8],
        amount: Amount,
    ) -> Result<(), Error> {
        trace!(
            "attempting to create a bmm request for block with hash = {} and with bribe = {}",
            critical_hash,
            amount
        );
        let mainchain_tip_hash = self.client.get_mainchain_tip()?;
        // Create a BMM request.
        let txid = self
            .client
            .send_bmm_request(critical_hash, &mainchain_tip_hash, 0, amount)?;
        let bmm_request = BMMRequest {
            txid,
            critical_hash: *critical_hash,
            side_block_data: block_data.to_vec(),
        };
        // and add request data to the requests vec.
        self.bmm_cache.requests.push(bmm_request);
        trace!("bmm request was created successfuly txid = {}", txid);
        Ok(())
    }

    // Check if any bmm request was accepted.
    pub fn confirm_bmm(&mut self) -> Result<Option<Block>, Error> {
        let mainchain_tip_hash = self.client.get_mainchain_tip()?;
        trace!(
            "attempting to confirm that a block was bmmed at mainchain tip = {}",
            &mainchain_tip_hash
        );
        if self.bmm_cache.prev_main_block_hash == mainchain_tip_hash {
            trace!("no new blocks on mainchain so sidechain block wasn't bmmed");
            // If no blocks were mined on mainchain no bmm requests could have
            // possibly been accepted.
            return Ok(None);
        }
        // Mainchain tip has changed so all requests for previous tip are now
        // invalid hence we update our prev_main_block_hash
        self.bmm_cache.prev_main_block_hash = mainchain_tip_hash;
        // and delete all requests with drain method.
        trace!("new blocks on mainchain, checking if sidechain block was bmmed");
        for request in self.bmm_cache.requests.drain(..) {
            // We check if our request was included in a mainchain block.
            if let Some(main_block_hash) = self.client.get_tx_block_hash(&request.txid)? {
                // And we check that critical_hash was actually included in
                // coinbase on mainchain.
                if let Ok(verified) = self
                    .client
                    .verify_bmm(&main_block_hash, &request.critical_hash)
                {
                    // If we find a bmm request that was accepted we return the
                    // corresponding block data.
                    let block = Block {
                        data: request.side_block_data,
                        time: verified.time,
                        main_block_hash,
                    };
                    info!(
                        "sidechain block {} was successfuly bmmed in mainchain block {} at {}",
                        &request.critical_hash,
                        &main_block_hash,
                        chrono::DateTime::<chrono::Utc>::from(
                            std::time::UNIX_EPOCH
                                + std::time::Duration::from_secs(verified.time as u64)
                        ),
                    );
                    return Ok(Some(block));
                }
            }
        }
        Ok(None)
    }

    pub fn format_deposit_address(&self, str_dest: &str) -> String {
        let deposit_address: String = format!("s{}_{}_", self.client.this_sidechain, str_dest);
        let hash = sha256::Hash::hash(deposit_address.as_bytes()).to_string();
        let hash: String = hash[..6].into();
        format!("{}{}", deposit_address, hash)
    }

    pub fn update_deposits(&self) -> Result<(), Error> {
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
    pub fn attempt_bundle_broadcast(&mut self) -> Result<(), Error> {
        trace!("attempting to create and broadcast a new bundle");
        self.update_bundles()?;
        // Wait for some time after a failed bundle to give people an
        // opportunity to refund. If we would create a new bundle immediately,
        // some outputs would be included in it immediately again, and so they
        // would never become refundable.
        //
        // We don't have to wait after a spent bundle, because, if mainchain
        // doesn't reorg, all withdrawal outputs in it will remain spent forever
        // anyway.
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
        info!("bundle {} created it is {}", bundle.txid(), status,);
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

    pub fn get_bundle_status(&self, txid: &Txid) -> Result<BundleStatus, Error> {
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
}

#[derive(Debug)]
pub struct BMMCache {
    requests: Vec<BMMRequest>,
    prev_main_block_hash: BlockHash,
}

impl BMMCache {
    fn new() -> BMMCache {
        BMMCache {
            requests: Vec::new(),
            prev_main_block_hash: BlockHash::default(),
        }
    }
}

#[derive(Debug)]
struct BMMRequest {
    txid: Txid,
    critical_hash: TxMerkleNode,
    side_block_data: Vec<u8>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum BundleStatus {
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
