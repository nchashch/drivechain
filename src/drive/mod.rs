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
use std::collections::HashMap;
pub use withdrawal::WithdrawalOutput;
use std::collections::HashSet;

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

impl Drivechain {
    pub fn new<P: AsRef<std::path::Path>>(
        db_path: P,
        this_sidechain: usize,
        rpcuser: String,
        rpcpassword: String,
    ) -> Drivechain {
        const LOCALHOST: &str = "127.0.0.1";
        const MAINCHAIN_PORT: usize = 18443;

        let client = DrivechainClient {
            this_sidechain,
            host: LOCALHOST.into(),
            port: MAINCHAIN_PORT,
            rpcuser,
            rpcpassword,
        };

        Drivechain {
            client,
            bmm_cache: BMMCache::new(),
            db: db::DB::new(db_path),
        }
    }

    pub fn get_coinbase_data(&self, prev_side_block_hash: BlockHash) -> CoinbaseData {
        let prev_main_block_hash = self
            .client
            .get_mainchain_tip()
            .expect("failed to get mainchain tip")
            .expect("no mainchain tip");
        CoinbaseData {
            prev_main_block_hash,
            prev_side_block_hash,
        }
    }

    // Attempts to blind merge mine a block.
    pub fn attempt_bmm(
        &mut self,
        critical_hash: &TxMerkleNode,
        block_data: &Vec<u8>,
        amount: Amount,
    ) {
        let mainchain_tip_hash = self
            .client
            .get_mainchain_tip()
            .expect("failed to get mainchain tip")
            .expect("no mainchain tip");
        // Create a BMM request.
        let txid = self
            .client
            .send_bmm_request(critical_hash, &mainchain_tip_hash, 0, amount)
            .unwrap();
        let bmm_request = BMMRequest {
            txid: txid,
            critical_hash: *critical_hash,
            side_block_data: block_data.to_vec(),
        };
        // and add request data to the requests vec.
        self.bmm_cache.requests.push(bmm_request);
    }

    // Check if any bmm request was accepted.
    pub fn confirm_bmm(&mut self) -> Option<Block> {
        let mainchain_tip_hash = self
            .client
            .get_mainchain_tip()
            .expect("failed to get mainchain tip")
            .expect("no mainchain tip");
        if self.bmm_cache.prev_main_block_hash == mainchain_tip_hash {
            // If no blocks were mined on mainchain no bmm requests could have
            // possibly been accepted.
            return None;
        }
        // Mainchain tip has changed so all requests for previous tip are now
        // invalid hence we update our prev_main_block_hash
        self.bmm_cache.prev_main_block_hash = mainchain_tip_hash;
        // and delete all requests with drain method.
        for request in self.bmm_cache.requests.drain(..) {
            // We check if our request was included in a mainchain block.
            if let Some(main_block_hash) = self.client.get_tx_block_hash(&request.txid).unwrap() {
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
                        main_block_hash: main_block_hash,
                    };
                    return Some(block);
                }
            }
        }
        None
    }

    pub fn format_deposit_address(&self, str_dest: &str) -> String {
        let deposit_address: String = format!("s{}_{}_", self.client.this_sidechain, str_dest);
        let hash = sha256::Hash::hash(deposit_address.as_bytes()).to_string();
        let hash: String = hash[..6].into();
        format!("{}{}", deposit_address, hash)
    }

    pub fn update_deposits(&self) {
        let mut last_deposit = self
            .db
            .get_last_deposit()
            .map(|(_, last_deposit)| last_deposit);
        while !last_deposit.clone().map_or(true, |last_deposit| {
            self.client.verify_deposit(&last_deposit).unwrap()
        }) {
            self.db.remove_last_deposit();
            last_deposit = self
                .db
                .get_last_deposit()
                .map(|(_, last_deposit)| last_deposit);
        }
        let last_output = last_deposit.map(|deposit| (deposit.tx.txid(), deposit.nburnindex));
        let deposits = self.client.get_deposits(last_output).unwrap();
        self.db.update_deposits(deposits.as_slice());
    }

    fn update_bundles(&mut self) {
        let known_failed = self.db.get_failed_bundle_hashes();
        let failed = self.client.get_failed_withdrawal_bundle_hashes().unwrap();
        let failed = failed.difference(&known_failed);
        for txid in failed {
            self.db.fail_bundle(txid);
        }
        let known_spent = self.db.get_spent_bundle_hashes();
        let spent = self.client.get_spent_withdrawal_bundle_hashes().unwrap();
        let spent = spent.difference(&known_spent);
        for txid in spent {
            self.db.spend_bundle(txid);
        }
        let voting = self.client.get_voting_withdrawal_bundle_hashes().unwrap();
        for txid in voting {
            self.db.vote_bundle(&txid);
        }
    }

    // TODO: Raise alarm if bundle hash being voted on is wrong.
    pub fn attempt_bundle_broadcast(&mut self) {
        {
            let bundles: HashMap<Txid, usize> = self
                .db
                .bundle_hash_to_inputs
                .iter()
                .map(|item| {
                    let (txid, inputs) = item.unwrap();
                    let mut txid_inner: [u8; 32] = Default::default();
                    txid_inner.copy_from_slice(&txid);
                    let txid = Txid::from_inner(txid_inner);
                    let inputs = bincode::deserialize::<Vec<Vec<u8>>>(&inputs).unwrap();
                    (txid, inputs.len())
                })
                .collect();
            dbg!(bundles);
        }
        self.update_bundles();
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
        let blocks_since_last_failed_bundle = self.db.get_blocks_since_last_failed_bundle();
        if blocks_since_last_failed_bundle < BUNDLE_WAIT_PERIOD {
            println!("last faild bundle was too soon");
            return;
        }
        let voting = self.client.get_voting_withdrawal_bundle_hashes().unwrap();
        // If a bundle is already being voted on we don't need to broadcast a
        // new one.
        if !voting.is_empty() {
            return;
        }
        let bundle = match self.db.create_bundle() {
            Some(bundle) => bundle,
            None => return,
        };
        dbg!(self.client.get_failed_withdrawal_bundle_hashes());
        dbg!(self.client.get_spent_withdrawal_bundle_hashes());
        dbg!(self.client.get_voting_withdrawal_bundle_hashes());
        dbg!(
            &bundle,
            bundle.txid(),
            self.get_bundle_status(&bundle.txid()),
        );
        let status = self.get_bundle_status(&bundle.txid());
        // We broadcast a bundle only if it was not seen before, meaning it is
        // neither failed nor spent.
        if status == BundleStatus::New {
            self.client
                .broadcast_withdrawal_bundle(&bundle)
                .expect("failed to broadcast bundle");
            self.db.vote_bundle(&bundle.txid());
        }
    }

    pub fn get_bundle_status(&self, txid: &Txid) -> BundleStatus {
        let voting = self.client.get_voting_withdrawal_bundle_hashes().unwrap();
        let failed = self.client.get_failed_withdrawal_bundle_hashes().unwrap();
        let spent = self.client.get_spent_withdrawal_bundle_hashes().unwrap();
        if voting.contains(txid) {
            BundleStatus::Voting
        } else if failed.contains(txid) {
            BundleStatus::Failed
        } else if spent.contains(txid) {
            BundleStatus::Spent
        } else {
            BundleStatus::New
        }
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
