use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

#[derive(Eq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct WithdrawalOutput {
    pub dest: [u8; 20],
    pub amount: u64,
    pub mainchain_fee: u64,
}

impl PartialEq for WithdrawalOutput {
    fn eq(&self, other: &Self) -> bool {
        self.mainchain_fee == other.mainchain_fee
            && self.dest == other.dest
            && self.amount == other.amount
    }
}

impl PartialOrd for WithdrawalOutput {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.mainchain_fee == other.mainchain_fee
            && self.dest == other.dest
            && self.amount == other.amount
        {
            Some(Ordering::Equal)
        } else if self.mainchain_fee < other.mainchain_fee
            || self.dest < other.dest
            || self.amount < other.amount
        {
            Some(Ordering::Less)
        } else {
            Some(Ordering::Greater)
        }
    }
}

impl Ord for WithdrawalOutput {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.mainchain_fee == other.mainchain_fee
            && self.dest == other.dest
            && self.amount == other.amount
        {
            Ordering::Equal
        } else if self.mainchain_fee < other.mainchain_fee
            || self.dest < other.dest
            || self.amount < other.amount
        {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}
