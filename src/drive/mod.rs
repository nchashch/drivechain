mod client;
mod coinbase_data;
mod db;
pub mod deposit;
pub mod withdrawal;
use bitcoin::blockdata::{
    opcodes, script,
    transaction::{Transaction, TxIn, TxOut},
};
use bitcoin::hash_types::{BlockHash, TxMerkleNode, Txid};
use bitcoin::hashes::{sha256, Hash};
use bitcoin::util::address::Address;
use bitcoin::util::amount::Amount;
use byteorder::{BigEndian, ByteOrder};
use client::DrivechainClient;
pub use coinbase_data::CoinbaseData;
pub use deposit::{Deposit};
use std::collections::HashMap;
use std::str::FromStr;
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

// The destination string for the change of a WT^
const SIDECHAIN_WTPRIME_RETURN_DEST: &[u8] = b"D";
const MAX_WT_OUTPUT_COUNT: usize = 100;
const WAITING_PERIOD: usize = 50;
const VOTING_PERIOD: usize = 50;

impl Drivechain {
    pub fn new<P: AsRef<std::path::Path>>(
        db_path: P,
        this_sidechain: usize,
        key_hash: String,
        rpcuser: String,
        rpcpassword: String,
    ) -> Drivechain {
        const LOCALHOST: &str = "127.0.0.1";
        const MAINCHAIN_PORT: usize = 18443;

        let client = DrivechainClient {
            this_sidechain: this_sidechain,
            key_hash: key_hash,
            host: LOCALHOST.into(),
            port: MAINCHAIN_PORT,
            rpcuser: rpcuser,
            rpcpassword: rpcpassword,
        };

        Drivechain {
            client: client,
            bmm_cache: BMMCache::new(),
            db: db::DB::new(db_path),
        }
    }

    pub fn get_coinbase_data(&self, prev_side_block_hash: BlockHash) -> CoinbaseData {
        let prev_main_block_hash = self
            .client
            .get_mainchain_tip()
            .expect("failed to get mainchain tip");
        let coinbase_data = CoinbaseData {
            prev_main_block_hash,
            prev_side_block_hash,
        };
        let bytes = coinbase_data.serialize();
        coinbase_data
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
            .expect("failed to get mainchain tip");
        // Create a BMM request.
        let txid = self
            .client
            .send_bmm_request(critical_hash, &mainchain_tip_hash, 0, amount);
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
            .expect("failed to get mainchain tip");
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
            if let Some(main_block_hash) = self.client.get_tx_block_hash(&request.txid) {
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
            self.client.verify_deposit(&last_deposit)
        }) {
            self.db.remove_last_deposit();
            last_deposit = self
                .db
                .get_last_deposit()
                .map(|(_, last_deposit)| last_deposit);
        }
        let last_output = last_deposit.map(|deposit| (deposit.tx.txid(), deposit.nburnindex));
        let deposits = self.client.get_deposits(last_output);
        self.db.update_deposits(deposits.as_slice());
    }

    // pub fn collect_wt_bundles(&self) -> Vec<Transaction> {
    //     let mut bundles = vec![];
    //     let last_height = match self
    //         .db
    //         .block_height_to_wtids
    //         .last()
    //         .expect("couldn't get last block number")
    //     {
    //         Some((last_height, _)) => last_height,
    //         None => {
    //             return vec![];
    //         }
    //     };
    //     let last_height = BigEndian::read_u64(&last_height) as usize;
    //     let last_height = get_waiting_end_height(last_height) + VOTING_PERIOD + WAITING_PERIOD;
    //     dbg!(last_height);
    //     let mut last_spent_height = 0;
    //     let mut waiting_end_height = get_waiting_end_height(last_spent_height);
    //     while waiting_end_height < last_height {
    //         dbg!(waiting_end_height);
    //         if let Some((spent_height, bundle)) =
    //             self.create_wt_bundle(last_spent_height, waiting_end_height, MAX_WT_OUTPUT_COUNT)
    //         {
    //             last_spent_height = spent_height;
    //             bundles.push(bundle);
    //         }
    //         waiting_end_height += VOTING_PERIOD + WAITING_PERIOD;
    //     }
    //     bundles
    // }

    // // This should be deterministic.
    // pub fn create_wt_bundle(
    //     &self,
    //     start: usize,
    //     end: usize,
    //     max_outputs: usize,
    // ) -> Option<(usize, Transaction)> {
    //     let mut transactions = HashMap::new();
    //     let mut total_outputs = 0;
    //     let mut last_spent_height = start;
    //     for block_height in start..end {
    //         if let Some(block_transactions) =
    //             self.db.get_block_withdrawal_transactions(block_height)
    //         {
    //             total_outputs += block_transactions.len();
    //             if total_outputs > max_outputs {
    //                 break;
    //             }
    //             let block_transactions = block_transactions.into_iter().filter(|(key, _)| {
    //                 self.db.get_withdrawal_status(*key) == Some(withdrawal::Status::Unspent)
    //             });
    //             transactions.extend(block_transactions);
    //             last_spent_height = block_height;
    //         }
    //     }
    //     if transactions.len() == 0 {
    //         return None;
    //     }
    //     dbg!(&transactions.len());
    //     let script = script::Builder::new()
    //         .push_opcode(opcodes::all::OP_RETURN)
    //         .push_slice(SIDECHAIN_WTPRIME_RETURN_DEST)
    //         .into_script();
    //     let mut txouts = vec![];
    //     let txout = TxOut {
    //         value: 0,
    //         script_pubkey: script,
    //     };
    //     txouts.push(txout);
    //     let sum_mainchain_fees: u64 = transactions
    //         .iter()
    //         .map(|(_, wt)| wt.iter().map(|out| out.mainchain_fee))
    //         .flatten()
    //         .sum();
    //     // Add an output for mainchain fee encoding (updated later)
    //     let script = script::Builder::new()
    //         .push_opcode(opcodes::all::OP_RETURN)
    //         .push_slice(sum_mainchain_fees.to_le_bytes().as_ref())
    //         .into_script();
    //     let txout = TxOut {
    //         value: 0,
    //         script_pubkey: script,
    //     };
    //     txouts.push(txout);
    //     let mut address_to_amount: HashMap<String, u64> = HashMap::new();
    //     for out in transactions.values().flatten() {
    //         let amount = address_to_amount.entry(out.dest.clone()).or_insert(0);
    //         *amount += out.amount;
    //     }
    //     txouts.extend(address_to_amount.iter().map(|(dest, amount)| TxOut {
    //         value: *amount,
    //         script_pubkey: Address::from_str(dest.as_str()).unwrap().script_pubkey(),
    //     }));
    //     let mut txin = TxIn::default();
    //     // OP_FALSE == OP_0
    //     txin.script_sig = script::Builder::new()
    //         .push_opcode(opcodes::OP_FALSE)
    //         .into_script();
    //     let tx = Transaction {
    //         version: 2,
    //         lock_time: 0,
    //         input: vec![txin],
    //         output: txouts,
    //     };
    //     Some((last_spent_height, tx))
    // }
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

#[derive(Debug)]
enum Status {
    Waiting,
    Voting,
}

fn get_waiting_end_height(block_height: usize) -> usize {
    let remainder = block_height % (WAITING_PERIOD + VOTING_PERIOD);
    block_height - remainder + WAITING_PERIOD
}

fn get_status(block_height: usize) -> Status {
    let remainder = block_height % (WAITING_PERIOD + VOTING_PERIOD);
    if remainder < WAITING_PERIOD {
        Status::Waiting
    } else {
        Status::Voting
    }
}
