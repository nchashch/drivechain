use crate::drive;
use bitcoin::hash_types::{BlockHash, TxMerkleNode};
use bitcoin::hashes::hex::ToHex;
use bitcoin::util::address::{Address, Payload};
use byteorder::{BigEndian, ByteOrder};
use std::collections::HashMap;
use std::str::FromStr;

#[cxx::bridge]
mod ffi {
    #[derive(Debug)]
    struct Block {
        data: String,
        time: i64,
        main_block_hash: String,
    }
    #[derive(Debug)]
    struct Output {
        address: String,
        amount: u64,
    }
    #[derive(Debug)]
    struct Withdrawal {
        outpoint: String,
        withdrawal_data: String,
        amount: u64,
    }
    extern "Rust" {
        type Drivechain;
        fn new_drivechain(
            db_path: &str,
            this_sidechain: usize,
            rpcuser: &str,
            rpcpassword: &str,
        ) -> Result<Box<Drivechain>>;

        fn get_coinbase_data(&self, prev_side_block_hash: &str) -> Result<Vec<u8>>;
        fn confirm_bmm(&mut self) -> Result<Vec<Block>>;
        fn attempt_bmm(&mut self, critical_hash: &str, block_data: &str, amount: u64)
            -> Result<()>;

        fn connect_withdrawals(&mut self, withdrawals: Vec<Withdrawal>) -> Result<bool>;
        fn disconnect_withdrawals(&mut self, outpoints: Vec<String>) -> Result<bool>;
        fn connect_refunds(&mut self, refund_outpoints: Vec<String>) -> Result<bool>;
        fn disconnect_refunds(&mut self, refund_outpoints: Vec<String>) -> Result<bool>;
        fn attempt_bundle_broadcast(&mut self) -> Result<()>;
        fn is_outpoint_spent(&self, outpoint: String) -> Result<bool>;
        fn connect_deposit_outputs(
            &mut self,
            outputs: Vec<Output>,
            just_check: bool,
        ) -> Result<bool>;
        fn disconnect_deposit_outputs(
            &mut self,
            outputs: Vec<Output>,
            just_check: bool,
        ) -> Result<bool>;
        fn verify_header_bmm(&self, main_block_hash: &str, critical_hash: &str) -> Result<bool>;
        fn verify_block_bmm(
            &self,
            main_block_hash: &str,
            critical_hash: &str,
            coinbase_data: &str,
        ) -> Result<bool>;
        fn get_deposit_outputs(&self) -> Result<Vec<Output>>;
        fn format_deposit_address(&self, address: &str) -> String;
        fn get_withdrawal_data(address: &str, fee: u64) -> Result<Vec<u8>>;
        fn flush(&mut self) -> Result<usize>;
    }
}

pub struct Drivechain(drive::Drivechain);

fn new_drivechain(
    db_path: &str,
    this_sidechain: usize,
    rpcuser: &str,
    rpcpassword: &str,
) -> color_eyre::Result<Box<Drivechain>, Error> {
    let drivechain =
        drive::Drivechain::new(db_path, this_sidechain, rpcuser.into(), rpcpassword.into())?;
    Ok(Box::new(Drivechain(drivechain)))
}

// FIXME: Check network here.
fn get_withdrawal_data(address: &str, fee: u64) -> color_eyre::Result<Vec<u8>, Error> {
    let address = Address::from_str(address)?;
    let hash = match address.payload {
        Payload::ScriptHash(hash) => hash,
        _ => return Err(Error::WrongAddressType),
    };
    let fee = fee.to_be_bytes().to_vec();
    let vec = [hash.to_vec(), fee].concat();
    Ok(vec)
}

impl Drivechain {
    fn get_coinbase_data(&self, prev_side_block_hash: &str) -> color_eyre::Result<Vec<u8>, Error> {
        let prev_side_block_hash = BlockHash::from_str(prev_side_block_hash)?;
        let coinbase_data = self.0.get_coinbase_data(prev_side_block_hash)?;
        Ok(coinbase_data.serialize())
    }

    fn confirm_bmm(&mut self) -> color_eyre::Result<Vec<ffi::Block>, Error> {
        let block = self.0.confirm_bmm()?;
        Ok(block
            .map(|block| ffi::Block {
                data: hex::encode(block.data),
                time: block.time,
                main_block_hash: block.main_block_hash.to_hex(),
            })
            .into_iter()
            .collect())
    }

    fn attempt_bmm(
        &mut self,
        critical_hash: &str,
        block_data: &str,
        amount: u64,
    ) -> color_eyre::Result<(), Error> {
        let critical_hash = TxMerkleNode::from_str(critical_hash)?;
        let block_data = hex::decode(block_data)?;
        let amount = bitcoin::Amount::from_sat(amount);
        self.0.attempt_bmm(&critical_hash, &block_data, amount)?;
        Ok(())
    }

    fn verify_header_bmm(
        &self,
        main_block_hash: &str,
        critical_hash: &str,
    ) -> color_eyre::Result<bool, Error> {
        let main_block_hash = BlockHash::from_str(main_block_hash)?;
        let critical_hash = TxMerkleNode::from_str(critical_hash)?;
        Ok(self
            .0
            .client
            .verify_bmm(&main_block_hash, &critical_hash)
            .is_ok())
    }

    fn verify_block_bmm(
        &self,
        main_block_hash: &str,
        critical_hash: &str,
        coinbase_data: &str,
    ) -> color_eyre::Result<bool, Error> {
        if !self.verify_header_bmm(main_block_hash, critical_hash)? {
            return Ok(false);
        }
        let main_block_hash = BlockHash::from_str(main_block_hash)?;
        let coinbase_data = hex::decode(coinbase_data)?;
        let coinbase_data = match drive::CoinbaseData::deserialize(&coinbase_data) {
            Some(cb) => cb,
            None => return Err(Error::CoinbaseDataDeserialize),
        };

        if let Some(prev_main_block_hash) = self
            .0
            .client
            .get_prev_block_hash(&main_block_hash)
            .map_err(|err| Error::Drive(err.into()))?
        {
            if prev_main_block_hash != coinbase_data.prev_main_block_hash {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
        Ok(true)
    }

    fn get_deposit_outputs(&self) -> color_eyre::Result<Vec<ffi::Output>, Error> {
        self.0.update_deposits()?;
        Ok(self
            .0
            .db
            .get_deposit_outputs()
            .map_err(|err| Error::Drive(err.into()))?
            .iter()
            .map(|output| ffi::Output {
                address: output.address.clone(),
                amount: output.amount,
            })
            .collect())
    }

    fn connect_withdrawals(
        &mut self,
        withdrawals: Vec<ffi::Withdrawal>,
    ) -> color_eyre::Result<bool, Error> {
        let withdrawals: Result<HashMap<Vec<u8>, drive::WithdrawalOutput>, Error> = withdrawals
            .into_iter()
            .map(|w| {
                let mut dest: [u8; 20] = Default::default();
                let withdrawal_data = hex::decode(w.withdrawal_data)?;
                dest.copy_from_slice(&withdrawal_data[0..20]);
                let mainchain_fee = &withdrawal_data[20..28];
                let mainchain_fee = BigEndian::read_u64(mainchain_fee);
                Ok((
                    hex::decode(w.outpoint)?,
                    drive::WithdrawalOutput {
                        amount: w.amount,
                        dest,
                        mainchain_fee,
                        // height is set later in Db::connect_withdrawals.
                        height: 0,
                    },
                ))
            })
            .collect();
        Ok(self.0.db.connect_withdrawals(withdrawals?).is_ok())
    }

    fn disconnect_withdrawals(
        &mut self,
        outpoints: Vec<String>,
    ) -> color_eyre::Result<bool, Error> {
        let outpoints: Result<Vec<Vec<u8>>, Error> = outpoints
            .iter()
            .map(|o| Ok(hex::decode(o)?.to_vec()))
            .collect();
        Ok(self.0.db.disconnect_withdrawals(outpoints?).is_ok())
    }

    fn connect_refunds(
        &mut self,
        refund_outpoints: Vec<String>,
    ) -> color_eyre::Result<bool, Error> {
        let refund_outpoints: Result<Vec<Vec<u8>>, Error> = refund_outpoints
            .iter()
            .map(|r| Ok(hex::decode(r)?.to_vec()))
            .collect();
        Ok(self.0.db.connect_refunds(refund_outpoints?).is_ok())
    }

    fn disconnect_refunds(
        &mut self,
        refund_outpoints: Vec<String>,
    ) -> color_eyre::Result<bool, Error> {
        let refund_outpoints: Result<Vec<Vec<u8>>, Error> = refund_outpoints
            .iter()
            .map(|r| Ok(hex::decode(r)?.to_vec()))
            .collect();
        Ok(self.0.db.disconnect_refunds(refund_outpoints?).is_ok())
    }

    fn attempt_bundle_broadcast(&mut self) -> color_eyre::Result<(), Error> {
        Ok(self.0.attempt_bundle_broadcast()?)
    }

    fn is_outpoint_spent(&self, outpoint: String) -> color_eyre::Result<bool, Error> {
        let outpoint = hex::decode(outpoint)?;
        self.0
            .db
            .is_outpoint_spent(outpoint)
            .map_err(|err| Error::Drive(err.into()))
    }

    fn connect_deposit_outputs(
        &mut self,
        outputs: Vec<ffi::Output>,
        just_check: bool,
    ) -> color_eyre::Result<bool, Error> {
        let outputs = outputs.iter().map(|output| drive::deposit::Output {
            address: output.address.clone(),
            amount: output.amount,
        });
        Ok(self
            .0
            .db
            .connect_deposit_outputs(outputs, just_check)
            .is_ok())
    }

    fn disconnect_deposit_outputs(
        &mut self,
        outputs: Vec<ffi::Output>,
        just_check: bool,
    ) -> color_eyre::Result<bool, Error> {
        let outputs = outputs.iter().map(|output| drive::deposit::Output {
            address: output.address.clone(),
            amount: output.amount,
        });
        Ok(self
            .0
            .db
            .disconnect_deposit_outputs(outputs, just_check)
            .is_ok())
    }

    fn format_deposit_address(&self, address: &str) -> String {
        self.0.format_deposit_address(address)
    }

    fn flush(&mut self) -> Result<usize, Error> {
        self.0.db.flush().map_err(|err| Error::Drive(err.into()))
    }
}

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("drivechain error")]
    Drive(#[from] drive::Error),
    #[error("hex error")]
    Hex(#[from] hex::FromHexError),
    #[error("bitcoin hex error")]
    BitcoinHex(#[from] bitcoin::hashes::hex::Error),
    #[error("bitcoin address error")]
    BitcoinAddress(#[from] bitcoin::util::address::Error),
    #[error("wrong address type")]
    WrongAddressType,
    #[error("failed to deserialize coinbase data")]
    CoinbaseDataDeserialize,
}
