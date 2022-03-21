#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WithdrawalOutput {
    pub refund_dest: String,
    pub dest: String,
    pub amount: u64,
    pub mainchain_fee: u64,
}

#[derive(PartialEq, Copy, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Status {
    Unspent,
    Pending,
    Spent,
    Refunded,
}

#[derive(Clone, Debug)]
pub enum Error {
    InvalidAddress(bitcoin::util::address::Error),
    WithdrawalAlreadyExists,
    WithdrawalDoesntExist,
}

impl From<bitcoin::util::address::Error> for Error {
    fn from(err: bitcoin::util::address::Error) -> Error {
        Error::InvalidAddress(err)
    }
}
