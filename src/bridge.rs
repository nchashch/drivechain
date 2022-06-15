use crate::drive;
use bitcoin::hash_types::{BlockHash, TxMerkleNode};
use bitcoin::hashes::hex::ToHex;
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
        fn connect_block(
            &mut self,
            deposits: Vec<Output>,
            withdrawals: Vec<Withdrawal>,
            refunds: Vec<String>,
            just_check: bool,
        ) -> Result<bool>;
        fn disconnect_block(
            &mut self,
            deposits: Vec<Output>,
            withdrawals: Vec<String>,
            refunds: Vec<String>,
            just_check: bool,
        ) -> Result<bool>;
        fn attempt_bundle_broadcast(&mut self) -> Result<()>;
        fn is_outpoint_spent(&self, outpoint: String) -> Result<bool>;
        fn is_main_block_connected(&self, main_block_hash: &str) -> Result<bool>;
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

fn get_withdrawal_data(address: &str, fee: u64) -> color_eyre::Result<Vec<u8>, Error> {
    let address = bitcoin::Address::from_str(address)?;
    let fee = bitcoin::Amount::from_sat(fee);
    drive::get_withdrawal_data(&address, &fee).map_err(|err| err.into())
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

    fn is_main_block_connected(&self, main_block_hash: &str) -> color_eyre::Result<bool, Error> {
        let main_block_hash = BlockHash::from_str(main_block_hash)?;
        self.0
            .is_main_block_connected(&main_block_hash)
            .map_err(|err| err.into())
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
            .verify_header_bmm(&main_block_hash, &critical_hash)
            .is_ok())
    }

    fn verify_block_bmm(
        &self,
        main_block_hash: &str,
        critical_hash: &str,
        coinbase_data: &str,
    ) -> color_eyre::Result<bool, Error> {
        let main_block_hash = BlockHash::from_str(main_block_hash)?;
        let critical_hash = TxMerkleNode::from_str(critical_hash)?;
        if self
            .0
            .verify_header_bmm(&main_block_hash, &critical_hash)
            .is_err()
        {
            return Ok(false);
        }
        let coinbase_data = hex::decode(coinbase_data)?;
        let coinbase_data = match drive::CoinbaseData::deserialize(&coinbase_data) {
            Some(cb) => cb,
            None => return Err(Error::CoinbaseDataDeserialize),
        };
        self.0
            .verify_block_bmm(&main_block_hash, &critical_hash, &coinbase_data)
            .map_err(|err| err.into())
    }

    fn get_deposit_outputs(&self) -> color_eyre::Result<Vec<ffi::Output>, Error> {
        self.0.update_deposits()?;
        Ok(self
            .0
            .get_deposit_outputs()?
            .iter()
            .map(|output| ffi::Output {
                address: output.address.clone(),
                amount: output.amount,
            })
            .collect())
    }

    fn attempt_bundle_broadcast(&mut self) -> color_eyre::Result<(), Error> {
        Ok(self.0.attempt_bundle_broadcast()?)
    }

    fn is_outpoint_spent(&self, outpoint: String) -> color_eyre::Result<bool, Error> {
        let outpoint = hex::decode(outpoint)?;
        self.0
            .is_outpoint_spent(outpoint.as_slice())
            .map_err(|err| err.into())
    }

    fn connect_block(
        &mut self,
        deposits: Vec<ffi::Output>,
        withdrawals: Vec<ffi::Withdrawal>,
        refunds: Vec<String>,
        just_check: bool,
    ) -> color_eyre::Result<bool, Error> {
        let deposits: Vec<drive::deposit::Output> = deposits
            .iter()
            .map(|output| drive::deposit::Output {
                address: output.address.clone(),
                amount: output.amount,
            })
            .collect();

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

        let refunds: Result<Vec<Vec<u8>>, Error> = refunds
            .iter()
            .map(|r| Ok(hex::decode(r)?.to_vec()))
            .collect();
        Ok(self
            .0
            .connect_block(
                deposits.as_slice(),
                &withdrawals?,
                refunds?.as_slice(),
                just_check,
            )
            .is_ok())
    }

    fn disconnect_block(
        &mut self,
        deposits: Vec<ffi::Output>,
        withdrawals: Vec<String>,
        refunds: Vec<String>,
        just_check: bool,
    ) -> color_eyre::Result<bool, Error> {
        let deposits: Vec<drive::deposit::Output> = deposits
            .iter()
            .map(|deposit| drive::deposit::Output {
                address: deposit.address.clone(),
                amount: deposit.amount,
            })
            .collect();
        let withdrawals: Result<Vec<Vec<u8>>, Error> = withdrawals
            .iter()
            .map(|o| Ok(hex::decode(o)?.to_vec()))
            .collect();
        let refunds: Result<Vec<Vec<u8>>, Error> = refunds
            .iter()
            .map(|r| Ok(hex::decode(r)?.to_vec()))
            .collect();
        Ok(self
            .0
            .disconnect_block(
                deposits.as_slice(),
                withdrawals?.as_slice(),
                refunds?.as_slice(),
                just_check,
            )
            .is_ok())
    }

    fn format_deposit_address(&self, address: &str) -> String {
        self.0.format_deposit_address(address)
    }

    fn flush(&mut self) -> Result<usize, Error> {
        self.0.flush().map_err(|err| err.into())
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
    #[error("failed to deserialize coinbase data")]
    CoinbaseDataDeserialize,
}
