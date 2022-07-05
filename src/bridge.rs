use crate::drive;
use bitcoin::hash_types::{BlockHash, TxMerkleNode};
use bitcoin::hashes::hex::ToHex;
use std::collections::HashMap;
use std::str::FromStr;

// FIXME: Move C++ FFI bindings code into a separate crate.
// FIXME: Figure out how to pass std::vector<unsigned char> directly, without
// hex encoding.
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
        main_address: String,
        main_fee: u64,
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
        fn get_mainchain_tip(&self) -> Result<Vec<u8>>;
        fn get_prev_main_block_hash(&self, main_block_hash: &str) -> Result<Vec<u8>>;
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
        fn is_outpoint_spent(&self, outpoint: &str) -> Result<bool>;
        fn is_main_block_connected(&self, main_block_hash: &str) -> Result<bool>;
        fn verify_bmm(&self, main_block_hash: &str, critical_hash: &str) -> Result<bool>;
        fn get_deposit_outputs(&self) -> Result<Vec<Output>>;
        fn format_deposit_address(&self, address: &str) -> String;
        fn extract_mainchain_address_bytes(address: &str) -> Result<Vec<u8>>;
        fn get_new_mainchain_address(&self) -> Result<String>;
        fn create_deposit(&self, address: &str, amount: u64, fee: u64) -> Result<String>;
        fn generate(&self, n: u64) -> Result<Vec<String>>;
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

impl Drivechain {
    fn get_mainchain_tip(&self) -> Result<Vec<u8>, Error> {
        let tip = self.0.get_mainchain_tip()?;
        Ok(tip.to_vec())
    }
    fn get_prev_main_block_hash(&self, main_block_hash: &str) -> Result<Vec<u8>, Error> {
        let main_block_hash = BlockHash::from_str(main_block_hash)?;
        let prev_hash = self.0.get_prev_main_block_hash(&main_block_hash)?;
        Ok(prev_hash.to_vec())
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

    fn verify_bmm(&self, main_block_hash: &str, critical_hash: &str) -> Result<bool, Error> {
        let main_block_hash = BlockHash::from_str(main_block_hash)?;
        let critical_hash = TxMerkleNode::from_str(critical_hash)?;
        Ok(self.0.verify_bmm(&main_block_hash, &critical_hash).is_ok())
    }

    fn get_deposit_outputs(&self) -> Result<Vec<ffi::Output>, Error> {
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

    fn is_outpoint_spent(&self, outpoint: &str) -> Result<bool, Error> {
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
                dest.copy_from_slice(hex::decode(w.main_address)?.as_slice());
                let mainchain_fee = w.main_fee;
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

    fn get_new_mainchain_address(&self) -> Result<String, Error> {
        let address = self.0.get_new_mainchain_address()?;
        Ok(address.to_string())
    }

    fn create_deposit(&self, address: &str, amount: u64, fee: u64) -> Result<String, Error> {
        self.0
            .create_deposit(
                address,
                bitcoin::Amount::from_sat(amount),
                bitcoin::Amount::from_sat(fee),
            )
            .map(|txid| txid.to_string())
            .map_err(|err| err.into())
    }

    fn generate(&self, n: u64) -> Result<Vec<String>, Error> {
        self.0
            .generate(n as usize)
            .map(|hashes| hashes.iter().map(|hash| hash.to_string()).collect())
            .map_err(|err| err.into())
    }

    fn flush(&mut self) -> Result<usize, Error> {
        self.0.flush().map_err(|err| err.into())
    }
}

fn extract_mainchain_address_bytes(address: &str) -> Result<Vec<u8>, Error> {
    let address = bitcoin::Address::from_str(&address)?;
    let bytes = drive::Drivechain::extract_mainchain_address_bytes(&address)?;
    Ok(bytes.to_vec())
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
}
