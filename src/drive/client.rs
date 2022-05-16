use base64::encode;
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::hash_types::{BlockHash, TxMerkleNode, Txid};
use bitcoin::util::amount::{Amount, Denomination};
use bitcoin::util::psbt::serialize::Deserialize;
use hyper::{Body, Client, Method, Request};
use serde::de::DeserializeOwned;
use serde_json::json;
use serde_json::value::Value;

use std::collections::HashSet;

use crate::drive::deposit::Deposit;
use std::str::FromStr;

use bitcoin::util::psbt::serialize::Serialize;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub struct DrivechainClient {
    pub this_sidechain: usize,
    pub key_hash: String,
    pub host: String,
    pub port: usize,
    pub rpcuser: String,
    pub rpcpassword: String,
}

#[derive(Debug)]
pub struct VerifiedBMM {
    pub time: i64,
    pub txid: Txid,
}

impl DrivechainClient {
    #[tokio::main]
    async fn send_request<T: DeserializeOwned>(
        &self,
        method: &str,
        params: &Vec<serde_json::Value>,
    ) -> Result<T, Error> {
        let auth = format!("{}:{}", self.rpcuser, self.rpcpassword);
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("http://{}:{}", self.host, self.port))
            .header("host", "127.0.0.1")
            .header("content-type", "application/json")
            .header("authorization", format!("Basic {}", encode(auth)))
            .header("connection", "close")
            .body(Body::from(
                json!({
                    "jsonrpc":"1.0",
                    "id": "SidechainClient",
                    "method": method,
                    "params": params}
                )
                .to_string(),
            ))?;

        let client = Client::new();

        let res = client.request(req).await?;
        let body = res.into_body();
        let body = hyper::body::to_bytes(body).await?;
        let body: Vec<u8> = body.into_iter().collect();
        let result: T = serde_json::from_slice::<T>(body.as_slice())?;
        return Ok(result);
    }

    // check that a block was successfuly bmmed
    pub fn verify_bmm(
        &self,
        main_block_hash: &BlockHash,
        critical_hash: &TxMerkleNode,
    ) -> Result<VerifiedBMM, Error> {
        let params = vec![
            json!(main_block_hash.to_string()),
            json!(critical_hash.to_string()),
        ];
        let value = self.send_request::<Value>("verifybmm", &params).unwrap();

        if value["error"] != Value::Null {
            let message = value["error"]["message"].as_str().unwrap();
            return Err(message.into());
        }
        let time = value["result"]["bmm"]["time"]
            .as_str()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        let txid = Txid::from_str(value["result"]["bmm"]["txid"].as_str().unwrap()).unwrap();
        Ok(VerifiedBMM {
            time: time,
            txid: txid,
        })
    }

    pub fn send_bmm_request(
        &self,
        critical_hash: &TxMerkleNode,
        prev_main_block_hash: &BlockHash,
        height: usize,
        amount: Amount,
    ) -> Txid {
        let str_hash_prev = prev_main_block_hash.to_string();
        let params = vec![
            json!(amount.to_string_in(Denomination::Bitcoin)),
            json!(height),
            json!(critical_hash.to_string()),
            json!(self.this_sidechain),
            json!(str_hash_prev[str_hash_prev.len() - 4..]),
        ];
        let value = self
            .send_request::<Value>("createbmmcriticaldatatx", &params)
            .unwrap();
        let txid = Txid::from_str(value["result"]["txid"]["txid"].as_str().unwrap()).unwrap();
        txid
    }

    // get active mainchain tip
    pub fn get_mainchain_tip(&self) -> Option<BlockHash> {
        let params = vec![];
        let value = self.send_request::<Value>("getchaintips", &params).unwrap();
        for tip in value["result"].as_array().unwrap() {
            if tip["status"] == "active" {
                let active_tip = BlockHash::from_str(tip["hash"].as_str().unwrap()).unwrap();
                return Some(active_tip);
            }
        }
        None
    }

    pub fn get_tx_block_hash(&self, txid: &Txid) -> Option<BlockHash> {
        let params = vec![json!(txid.to_string())];
        let value = self
            .send_request::<Value>("gettransaction", &params)
            .unwrap();
        if let Some(block_hash) = value["result"].get("blockhash") {
            Some(BlockHash::from_str(block_hash.as_str().unwrap()).unwrap())
        } else {
            None
        }
    }

    pub fn get_prev_block_hash(&self, block_hash: &BlockHash) -> Option<BlockHash> {
        let params = vec![json!(block_hash.to_string())];
        let value = self.send_request::<Value>("getblock", &params).unwrap();
        let prev_block_hash = &value["result"]["previousblockhash"];
        Some(BlockHash::from_str(prev_block_hash.as_str().unwrap()).unwrap())
    }

    pub fn get_block_count(&self) -> usize {
        let params = vec![];
        let value = self
            .send_request::<Value>("getblockcount", &params)
            .unwrap();
        let count = value["result"].as_u64().unwrap();
        count as usize
    }

    pub fn get_block_hash(&self, height: usize) -> BlockHash {
        let params = vec![json!(height)];
        let value = self.send_request::<Value>("getblockhash", &params).unwrap();
        let block_hash = BlockHash::from_str(value["result"].as_str().unwrap()).unwrap();
        block_hash
    }

    pub fn get_deposits(&self, last_deposit: Option<(Txid, usize)>) -> Vec<Deposit> {
        let params = match last_deposit {
            Some((txid, nburnindex)) => vec![
                json!(self.key_hash),
                json!(txid.to_string()),
                json!(nburnindex),
            ],
            None => vec![json!(self.key_hash)],
        };
        let value = self
            .send_request::<Value>("listsidechaindeposits", &params)
            .unwrap();
        if let Some(result) = value["result"].as_array() {
            return result
                .iter()
                .map(|val| Deposit {
                    blockhash: BlockHash::from_str(val["hashblock"].as_str().unwrap()).unwrap(),
                    ntx: val["ntx"].as_u64().unwrap() as usize,
                    nburnindex: val["nburnindex"].as_u64().unwrap() as usize,
                    tx: Transaction::deserialize(
                        hex::decode(val["txhex"].as_str().unwrap())
                            .unwrap()
                            .as_slice(),
                    )
                    .unwrap(),
                    nsidechain: val["nsidechain"].as_u64().unwrap() as usize,
                    strdest: val["strdest"].as_str().unwrap().into(),
                })
                .collect();
        }
        vec![]
    }

    pub fn verify_deposit(&self, deposit: &Deposit) -> bool {
        let params = vec![
            json!(deposit.blockhash.to_string()),
            json!(deposit.tx.txid().to_string()),
            json!(deposit.ntx),
        ];
        let value = self
            .send_request::<Value>("verifydeposit", &params)
            .unwrap();
        if let Some(txid) = value.get("result") {
            if txid.is_null() {
                return false;
            }
            let txid = Txid::from_str(txid.as_str().unwrap()).unwrap();
            deposit.tx.txid() == txid
        } else {
            false
        }
    }

    pub fn broadcast_withdrawal_bundle(&self, wttx: &Transaction) -> Option<Txid> {
        let params = vec![
            json!(self.this_sidechain),
            json!(hex::encode(wttx.serialize())),
        ];
        let value = self
            .send_request::<Value>("receivewithdrawalbundle", &params)
            .unwrap();
        value["result"]
            .get("wtxid")
            .map(|txid| Txid::from_str(txid.as_str().unwrap()).unwrap())
    }

    pub fn is_bundle_spent(&self, txid: &Txid) -> bool {
        let params = vec![
            json!(txid.to_string()),
            json!(self.this_sidechain),
        ];
        let value = self.send_request::<Value>("havespentwithdrawal", &params).unwrap();
        value["result"].as_bool().unwrap()
    }

    pub fn is_bundle_failed(&self, txid: &Txid) -> bool {
        let params = vec![
            json!(txid.to_string()),
            json!(self.this_sidechain),
        ];
        let value = self.send_request::<Value>("havefailedwithdrawal", &params).unwrap();
        value["result"].as_bool().unwrap()
    }

    pub fn get_failed_withdrawal_bundle_hashes(&self) -> HashSet<Txid> {
        let params = vec![];
        let value = self
            .send_request::<Value>("listfailedwithdrawals", &params)
            .unwrap();
        if let Some(result) = value["result"].as_array() {
            return result
                .iter()
                .filter(|v| v["nsidechain"].as_u64().unwrap() as usize == self.this_sidechain)
                .map(|v| Txid::from_str(v["hash"].as_str().unwrap()).unwrap())
                .collect();
        }
        HashSet::new()
    }

    pub fn get_spent_withdrawal_bundle_hashes(&self) -> HashSet<Txid> {
        let params = vec![];
        let value = self
            .send_request::<Value>("listspentwithdrawals", &params)
            .unwrap();
        if let Some(result) = value["result"].as_array() {
            return result
                .iter()
                .filter(|v| v["nsidechain"].as_u64().unwrap() as usize == self.this_sidechain)
                .map(|v| Txid::from_str(v["hash"].as_str().unwrap()).unwrap())
                .collect();
        }
        HashSet::new()
    }

    pub fn get_voting_withdrawal_bundle_hashes(&self) -> HashSet<Txid> {
        let params = vec![json!(self.this_sidechain)];
        let value = self
            .send_request::<Value>("listwithdrawalstatus", &params)
            .unwrap();
        if let Some(result) = value["result"].as_array() {
            return result
                .iter()
                .map(|v| Txid::from_str(v["hash"].as_str().unwrap()).unwrap())
                .collect();
        }
        HashSet::new()
    }
}
