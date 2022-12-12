// FIXME: This implementaiton of a JSON RPC client is too low level. Restructure
// this.
use crate::drive::deposit::MainDeposit;
use base64::encode;
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::hash_types::{BlockHash, Txid};
use bitcoin::util::amount::{Amount, Denomination};
use bitcoin::util::psbt::serialize::Deserialize;
use log::{info, trace};
use std::collections::HashSet;
use std::str::FromStr;
use ureq::json;

use bitcoin::util::psbt::serialize::Serialize;

// TODO: Implement mock client for running unit tests.
pub struct Client {
    pub this_sidechain: usize,
    pub host: String,
    pub port: u16,
    pub rpcuser: String,
    pub rpcpassword: String,
}

#[derive(Debug)]
pub struct VerifiedBMM {
    pub time: i64,
    pub txid: Txid,
}

impl Client {
    fn send_request(
        &self,
        method: &str,
        params: &Vec<ureq::serde_json::Value>,
    ) -> Result<ureq::serde_json::Value, Error> {
        let auth = format!("{}:{}", self.rpcuser, self.rpcpassword);
        let resp = ureq::post(format!("http://{}:{}", self.host, self.port).as_str())
            .set("host", "127.0.0.1")
            .set("content-type", "application/json")
            .set("authorization", format!("Basic {}", encode(auth)).as_str())
            .set("connection", "close")
            .send_json(json!({
                "jsonrpc":"1.0",
                "id": "SidechainClient",
                "method": method,
                "params": params}
            ))
            // At this point the request can fail and return a ureq::Error, we
            // wrap it with our Error type.
            .map_err(Error::Ureq)
            // Now we have a Result<ureq::Response, Error> type.
            //
            // Here if the request succeeded we convert the body into json, and
            // if the conversion fails we wrap the std::io::Error type in our
            // Error type.
            //
            // So at this point we have a Result<Result<Value, Error>, Error> type.
            .map(|resp| resp.into_json().map_err(Error::Io))
            // Turn a Result<Result<Value, Error>, Error> into Result<Value, Error>
            //
            // Cannot use flatten because it is unstable for Result type.
            .unwrap_or_else(Err);
        resp
    }

    pub fn is_main_block_connected(&self, main_block_hash: &BlockHash) -> Result<bool, Error> {
        trace!(
            "checking if mainchain block {} is part of consensus",
            main_block_hash
        );
        let params = vec![json!(main_block_hash.to_string())];
        self.send_request("getblock", &params)
            .map(|value| {
                let confirmations = match value["result"]["confirmations"].as_i64() {
                    Some(c) => c,
                    None => return Err(Error::JsonSchema),
                };
                Ok(confirmations != -1)
            })
            .unwrap_or_else(Err)
    }

    pub fn get_main_block_height(&self, main_block_hash: &BlockHash) -> Result<usize, Error> {
        let params = vec![json!(main_block_hash.to_string())];
        self.send_request("getblock", &params)
            .map(|value| {
                let height = match value["result"]["height"].as_u64() {
                    Some(c) => c,
                    None => return Err(Error::JsonSchema),
                };
                Ok(height as usize)
            })
            .unwrap_or_else(Err)
    }

    pub fn get_next_main_block(
        &self,
        prev_main_block_hash: &BlockHash,
    ) -> Result<BlockHash, Error> {
        let params = vec![json!(prev_main_block_hash.to_string())];
        self.send_request("getblock", &params).map(|value| {
            match &value["result"]["nextblockhash"] {
                ureq::serde_json::Value::String(main_block_hash) => {
                    Ok(BlockHash::from_str(main_block_hash).map_err(ParseError::BitcoinHex)?)
                }
                _ => Err(Error::NoNextBlock),
            }
        })?
    }

    // check that a block was successfuly bmmed
    pub fn verify_bmm(
        &self,
        main_block_hash: &BlockHash,
        side_block_hash: &BlockHash,
    ) -> Result<VerifiedBMM, Error> {
        let params = vec![
            json!(main_block_hash.to_string()),
            json!(side_block_hash.to_string()),
            json!(self.this_sidechain),
        ];
        trace!(
            "verifying bmm main block hash = {}, side block hash = {}",
            main_block_hash,
            side_block_hash
        );
        self.send_request("verifybmm", &params)
            .map(|value| {
                if value["error"] != ureq::serde_json::Value::Null {
                    let message = match value["error"]["message"].as_str() {
                        Some(message) => message,
                        None => return Err(Error::JsonSchema),
                    };
                    return Err(RpcError::InvalidBmm(message.into()).into());
                }
                // Time shows up as a string in json response, not as a number.
                let time = match value["result"]["bmm"]["time"].as_str() {
                    Some(time) => time,
                    None => return Err(Error::JsonSchema),
                };
                let time = match time.parse::<i64>() {
                    Ok(time) => time,
                    Err(err) => return Err(Error::Parse(err.into())),
                };
                let txid = match value["result"]["bmm"]["txid"].as_str() {
                    Some(txid) => txid,
                    None => return Err(Error::JsonSchema),
                };
                let txid = match Txid::from_str(txid) {
                    Ok(txid) => txid,
                    Err(err) => return Err(Error::Parse(err.into())),
                };
                trace!("bmm is valid txid = {}, time = {}", txid, time);
                Ok(VerifiedBMM { time, txid })
            })
            .unwrap_or_else(Err)
    }

    pub fn send_bmm_request(
        &self,
        side_block_hash: &BlockHash,
        prev_main_block_hash: &BlockHash,
        height: usize,
        amount: Amount,
    ) -> Result<Txid, Error> {
        trace!(
            "sending bmm request critical hash = {}, prev main hash = {}, height = {}, amount = {}",
            side_block_hash,
            prev_main_block_hash,
            height,
            amount
        );
        let str_hash_prev = prev_main_block_hash.to_string();
        let params = vec![
            json!(amount.to_string_in(Denomination::Bitcoin)),
            json!(height),
            json!(side_block_hash.to_string()),
            json!(self.this_sidechain),
            json!(str_hash_prev[str_hash_prev.len() - 8..]),
        ];
        self.send_request("createbmmcriticaldatatx", &params)
            .map(|value| {
                let txid = match value["result"]["txid"]["txid"].as_str() {
                    Some(txid) => txid,
                    None => return Err(Error::JsonSchema),
                };
                let txid = match Txid::from_str(txid) {
                    Ok(txid) => txid,
                    Err(err) => return Err(Error::Parse(err.into())),
                };
                trace!("bmm request sent successfuly txid = {}", txid);
                Ok(txid)
            })
            .unwrap_or_else(Err)
    }

    // get active mainchain tip
    pub fn get_mainchain_tip(&self) -> Result<BlockHash, Error> {
        let params = vec![];
        self.send_request("getbestblockhash", &params)
            .map(|value| match &value["result"] {
                ureq::serde_json::Value::String(main_block_hash) => {
                    Ok(BlockHash::from_str(main_block_hash).map_err(ParseError::BitcoinHex)?)
                }
                _ => Err(RpcError::NoMainchainTip.into()),
            })
            .unwrap_or_else(Err)
    }

    pub fn get_tx_block_hash(&self, txid: &Txid) -> Result<Option<BlockHash>, Error> {
        let params = vec![json!(txid.to_string())];
        self.send_request("gettransaction", &params)
            .map(|value| {
                if let Some(block_hash) = value["result"].get("blockhash") {
                    let block_hash = match block_hash.as_str() {
                        Some(bh) => bh,
                        None => return Err(Error::JsonSchema),
                    };
                    let block_hash = match BlockHash::from_str(block_hash) {
                        Ok(bh) => bh,
                        Err(err) => return Err(Error::Parse(err.into())),
                    };
                    Ok(Some(block_hash))
                } else {
                    Ok(None)
                }
            })
            .unwrap_or_else(Err)
    }

    pub fn get_prev_block_hash(&self, block_hash: &BlockHash) -> Result<BlockHash, Error> {
        let params = vec![json!(block_hash.to_string())];
        self.send_request("getblock", &params)
            .map(|value| {
                let prev_block_hash = match value["result"].get("previousblockhash") {
                    Some(pbh) => pbh,
                    None => return Err(Error::GenesisBlock),
                };
                let prev_block_hash = match prev_block_hash.as_str() {
                    Some(pbh) => pbh,
                    None => return Err(Error::JsonSchema),
                };
                let prev_block_hash = match BlockHash::from_str(prev_block_hash) {
                    Ok(pbh) => pbh,
                    Err(err) => return Err(Error::Parse(err.into())),
                };
                Ok(prev_block_hash)
            })
            .unwrap_or_else(Err)
    }

    pub fn get_deposits(
        &self,
        last_deposit: Option<(Txid, usize)>,
    ) -> Result<Vec<MainDeposit>, Error> {
        trace!(
            "requesting deposits since last deposit = {:?}",
            last_deposit
        );
        let params = match last_deposit {
            Some((txid, nburnindex)) => vec![
                json!(self.this_sidechain),
                json!(txid.to_string()),
                json!(nburnindex),
            ],
            None => vec![json!(self.this_sidechain)],
        };
        self.send_request("listsidechaindeposits", &params)
            .map(|value| {
                let result: Result<Vec<MainDeposit>, Error> = match value["result"].as_array() {
                    Some(result) => result
                        .iter()
                        .map(|val| {
                            let blockhash = match val["hashblock"].as_str() {
                                Some(bh) => bh,
                                None => return Err(Error::JsonSchema),
                            };
                            let blockhash = match BlockHash::from_str(blockhash) {
                                Ok(bh) => bh,
                                Err(err) => return Err(Error::Parse(err.into())),
                            };
                            let ntx = match val["ntx"].as_u64() {
                                Some(ntx) => ntx as usize,
                                None => return Err(Error::JsonSchema),
                            };
                            let nburnindex = match val["nburnindex"].as_u64() {
                                Some(nbi) => nbi as usize,
                                None => return Err(Error::JsonSchema),
                            };
                            let tx = match val["txhex"].as_str() {
                                Some(tx) => tx,
                                None => return Err(Error::JsonSchema),
                            };
                            let tx = match hex::decode(tx) {
                                Ok(tx) => tx,
                                Err(err) => return Err(Error::Parse(err.into())),
                            };
                            let tx = match Transaction::deserialize(tx.as_slice()) {
                                Ok(tx) => tx,
                                Err(err) => return Err(Error::Parse(err.into())),
                            };
                            let nsidechain = match val["nsidechain"].as_u64() {
                                Some(ns) => ns as usize,
                                None => return Err(Error::JsonSchema),
                            };
                            let strdest = match val["strdest"].as_str() {
                                Some(sd) => sd.into(),
                                None => return Err(Error::JsonSchema),
                            };
                            Ok(MainDeposit {
                                blockhash,
                                ntx,
                                nburnindex,
                                tx,
                                nsidechain,
                                strdest,
                            })
                        })
                        .collect(),
                    None => return Err(Error::JsonSchema),
                };
                match &result {
                    Ok(deposits) => trace!("got {} new deposits", deposits.len()),
                    Err(err) => info!("failed to get new deposits with error = {}", err),
                };
                result
            })
            .unwrap_or_else(Err)
            // FIXME: Right now mainchain returns an error if there are no
            // deposits in it's db instead of an empty array, so we have to make
            // this exception here.
            .or_else(|err| match err {
                Error::Ureq(_) => Ok(vec![]),
                err => Err(err),
            })
    }

    pub fn verify_deposit(&self, deposit: &MainDeposit) -> Result<bool, Error> {
        trace!("verifying deposit {:?}", deposit);
        let params = vec![
            json!(deposit.blockhash.to_string()),
            json!(deposit.tx.txid().to_string()),
            json!(deposit.ntx),
        ];
        self.send_request("verifydeposit", &params)
            .map(|value| match value.get("result") {
                Some(txid) => {
                    if txid.is_null() {
                        return Ok(false);
                    }
                    let txid = match txid.as_str() {
                        Some(txid) => txid,
                        None => return Err(Error::JsonSchema),
                    };
                    let txid = match Txid::from_str(txid) {
                        Ok(txid) => txid,
                        Err(err) => return Err(Error::Parse(err.into())),
                    };
                    if deposit.tx.txid() == txid {
                        trace!("deposit is valid");
                    } else {
                        trace!("deposit is invalid");
                    }
                    Ok(deposit.tx.txid() == txid)
                }
                None => Err(RpcError::InvalidDeposit.into()),
            })
            .unwrap_or_else(Err)
    }

    pub fn broadcast_withdrawal_bundle(&self, wttx: &Transaction) -> Result<Option<Txid>, Error> {
        trace!("broadcasting withdrawal bundle {}", wttx.txid());
        let params = vec![
            json!(self.this_sidechain),
            json!(hex::encode(wttx.serialize())),
        ];
        self.send_request("receivewithdrawalbundle", &params)
            .map(|value| {
                value["result"]
                    .get("wtxid")
                    .map(|txid| {
                        let txid = match txid.as_str() {
                            Some(txid) => txid,
                            None => return Err(Error::JsonSchema),
                        };
                        Txid::from_str(txid).map_err(|err| Error::Parse(err.into()))
                    })
                    .transpose()
            })
            .unwrap_or_else(Err)
    }

    pub fn get_failed_withdrawal_bundle_hashes(&self) -> Result<HashSet<Txid>, Error> {
        let params = vec![];
        self.send_request("listfailedwithdrawals", &params)
            .map(|value| match value["result"].as_array() {
                Some(result) => {
                    let pairs = Self::collect_nsidechain_txid_pairs(result);
                    // Map over the Result<...> we have.
                    pairs.map(|pairs| {
                        pairs
                            .into_iter()
                            // Filter out all txids from other sidechains
                            .filter(|(nsidechain, _)| *nsidechain == self.this_sidechain)
                            // Finally only return txids.
                            .map(|(_, txid)| txid)
                            .collect()
                    })
                }
                None => Err(Error::JsonSchema),
            })
            .unwrap_or_else(Err)
            .or_else(|err| match err {
                Error::Ureq(_) => Ok(HashSet::new()),
                err => Err(err),
            })
    }

    pub fn get_spent_withdrawal_bundle_hashes(&self) -> Result<HashSet<Txid>, Error> {
        let params = vec![];
        self.send_request("listspentwithdrawals", &params)
            .map(|value| match value["result"].as_array() {
                Some(result) => {
                    let pairs = Self::collect_nsidechain_txid_pairs(result);
                    pairs.map(|pairs| {
                        pairs
                            .into_iter()
                            .filter(|(nsidechain, _)| *nsidechain == self.this_sidechain)
                            .map(|(_, txid)| txid)
                            .collect()
                    })
                }
                None => Err(Error::JsonSchema),
            })
            .unwrap_or_else(Err)
            .or_else(|err| match err {
                Error::Ureq(_) => Ok(HashSet::new()),
                err => Err(err),
            })
    }

    pub fn get_new_mainchain_address(&self) -> Result<bitcoin::Address, Error> {
        let params = vec![];
        self.send_request("getnewaddress", &params)
            .map(|value| match value["result"].as_str() {
                Some(address) => {
                    Ok(bitcoin::Address::from_str(address).map_err(ParseError::BitcoinAddress)?)
                }
                None => Err(Error::JsonSchema),
            })
            .unwrap_or_else(Err)
    }

    pub fn get_voting_withdrawal_bundle_hashes(&self) -> Result<HashSet<Txid>, Error> {
        let params = vec![json!(self.this_sidechain)];
        self.send_request("listwithdrawalstatus", &params)
            .map(|value| match value["result"].as_array() {
                Some(result) => result
                    .iter()
                    .map(|v| {
                        let txid = match v["hash"].as_str() {
                            Some(txid) => txid,
                            None => return Err(Error::JsonSchema),
                        };
                        Txid::from_str(txid).map_err(|err| Error::Parse(err.into()))
                    })
                    .collect(),
                None => Err(Error::JsonSchema),
            })
            .unwrap_or_else(Err)
            .or_else(|err| match err {
                Error::Ureq(_) => Ok(HashSet::new()),
                err => Err(err),
            })
    }

    pub fn create_sidechain_deposit(
        &self,
        address: &str,
        amount: Amount,
        fee: Amount,
    ) -> Result<Txid, Error> {
        let params = vec![
            json!(self.this_sidechain),
            json!(address),
            json!(amount.to_string_in(bitcoin::Denomination::Bitcoin)),
            json!(fee.to_string_in(bitcoin::Denomination::Bitcoin)),
        ];
        self.send_request("createsidechaindeposit", &params)
            .map(|value| match value["result"].as_str() {
                Some(txid) => Ok(Txid::from_str(txid).map_err(ParseError::BitcoinHex)?),
                None => Err(Error::JsonSchema),
            })
            .unwrap_or_else(Err)
    }

    pub fn generate(&self, n: usize) -> Result<Vec<BlockHash>, Error> {
        let params = vec![json!(n)];
        self.send_request("generate", &params)
            .map(|value| match value["result"].as_array() {
                Some(hashes) => hashes
                    .iter()
                    .map(|hash| match hash.as_str() {
                        Some(hash) => {
                            Ok(BlockHash::from_str(hash).map_err(ParseError::BitcoinHex)?)
                        }
                        None => Err(Error::JsonSchema),
                    })
                    .collect(),
                None => Err(Error::JsonSchema),
            })
            .unwrap_or_else(Err)
    }

    fn collect_nsidechain_txid_pairs(
        array: &[ureq::serde_json::Value],
    ) -> Result<Vec<(usize, Txid)>, Error> {
        array
            .iter()
            .map(|v| {
                let nsidechain = match v["nsidechain"].as_u64() {
                    Some(nsidechain) => nsidechain as usize,
                    None => return Err(Error::JsonSchema),
                };
                let txid = match v["hash"].as_str() {
                    Some(txid) => txid,
                    None => return Err(Error::JsonSchema),
                };
                let txid = match Txid::from_str(txid) {
                    Ok(txid) => txid,
                    Err(err) => return Err(Error::Parse(err.into())),
                };
                Ok((nsidechain, txid))
            })
            .collect::<Result<Vec<(usize, Txid)>, Error>>()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    #[error("bitcoin address parse error")]
    BitcoinAddress(#[from] bitcoin::util::address::Error),
    #[error("bitcoin parse error")]
    Bitcoin(#[from] bitcoin::consensus::encode::Error),
    #[error("int parse error")]
    Int(#[from] std::num::ParseIntError),
    #[error("hex parse error")]
    Hex(#[from] hex::FromHexError),
    #[error("bitcoin hex parse error")]
    BitcoinHex(#[from] bitcoin::hashes::hex::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum RpcError {
    #[error("no mainchain tip")]
    NoMainchainTip,
    #[error("failed to verify deposit")]
    InvalidDeposit,
    #[error("failed to verify bmm: {0}")]
    InvalidBmm(String),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("json parse error")]
    Io(#[from] std::io::Error),
    #[error("ureq error")]
    Ureq(#[from] ureq::Error),
    #[error("parse error")]
    Parse(#[from] ParseError),
    #[error("rpc error")]
    Rpc(#[from] RpcError),
    #[error("json schema error")]
    JsonSchema,
    #[error("next block wasn't mined yet")]
    NoNextBlock,
    #[error("genesis block doesn't have previous block")]
    GenesisBlock,
}
