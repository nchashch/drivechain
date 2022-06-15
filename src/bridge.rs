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
        fn get_prev_block_hashes(&self, prev_side_block_hash: &str) -> Result<Vec<u8>>;
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
            prev_block_hashes: &str,
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
) -> Result<Box<Drivechain>, Error> {
    let drivechain =
        drive::Drivechain::new(db_path, this_sidechain, rpcuser.into(), rpcpassword.into())?;
    Ok(Box::new(Drivechain(drivechain)))
}

fn get_withdrawal_data(address: &str, fee: u64) -> Result<Vec<u8>, Error> {
    let address = bitcoin::Address::from_str(address)?;
    let fee = bitcoin::Amount::from_sat(fee);
    drive::Drivechain::get_withdrawal_data(&address, &fee).map_err(|err| err.into())
}

impl Drivechain {
    fn get_prev_block_hashes(&self, prev_side_block_hash: &str) -> Result<Vec<u8>, Error> {
        let prev_side_block_hash = BlockHash::from_str(prev_side_block_hash)?;
        let prev_block_hashes = self.0.get_prev_block_hashes(prev_side_block_hash)?;
        Ok(prev_block_hashes.serialize())
    }

    fn confirm_bmm(&mut self) -> Result<Vec<ffi::Block>, Error> {
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
    ) -> Result<(), Error> {
        let critical_hash = TxMerkleNode::from_str(critical_hash)?;
        let block_data = hex::decode(block_data)?;
        let amount = bitcoin::Amount::from_sat(amount);
        self.0.attempt_bmm(&critical_hash, &block_data, amount)?;
        Ok(())
    }

    fn is_main_block_connected(&self, main_block_hash: &str) -> Result<bool, Error> {
        let main_block_hash = BlockHash::from_str(main_block_hash)?;
        self.0
            .is_main_block_connected(&main_block_hash)
            .map_err(|err| err.into())
    }

    fn verify_header_bmm(&self, main_block_hash: &str, critical_hash: &str) -> Result<bool, Error> {
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
        prev_block_hashes: &str,
    ) -> Result<bool, Error> {
        let main_block_hash = BlockHash::from_str(main_block_hash)?;
        let critical_hash = TxMerkleNode::from_str(critical_hash)?;
        if self
            .0
            .verify_header_bmm(&main_block_hash, &critical_hash)
            .is_err()
        {
            return Ok(false);
        }
        let prev_block_hashes = hex::decode(prev_block_hashes)?;
        let prev_block_hashes = match drive::PrevBlockHashes::deserialize(&prev_block_hashes) {
            Some(cb) => cb,
            None => return Err(Error::PrevBlockHashesDeserialize),
        };
        self.0
            .verify_block_bmm(&main_block_hash, &critical_hash, &prev_block_hashes)
            .map_err(|err| err.into())
    }

    fn get_deposit_outputs(&self) -> Result<Vec<ffi::Output>, Error> {
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

    fn attempt_bundle_broadcast(&mut self) -> Result<(), Error> {
        Ok(self.0.attempt_bundle_broadcast()?)
    }

    fn is_outpoint_spent(&self, outpoint: String) -> Result<bool, Error> {
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
    ) -> Result<bool, Error> {
        let deposits: Vec<drive::Deposit> = deposits
            .iter()
            .map(|output| drive::Deposit {
                address: output.address.clone(),
                amount: output.amount,
            })
            .collect();

        let withdrawals: Result<HashMap<Vec<u8>, drive::Withdrawal>, Error> = withdrawals
            .into_iter()
            .map(|w| {
                let mut dest: [u8; 20] = Default::default();
                let withdrawal_data = hex::decode(w.withdrawal_data)?;
                dest.copy_from_slice(&withdrawal_data[0..20]);
                let mainchain_fee = &withdrawal_data[20..28];
                let mainchain_fee = BigEndian::read_u64(mainchain_fee);
                Ok((
                    hex::decode(w.outpoint)?,
                    drive::Withdrawal {
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
    ) -> Result<bool, Error> {
        let deposits: Vec<drive::Deposit> = deposits
            .iter()
            .map(|deposit| drive::Deposit {
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
    PrevBlockHashesDeserialize,
}
