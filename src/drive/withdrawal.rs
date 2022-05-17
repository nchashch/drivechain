use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

#[derive(Eq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct WithdrawalOutput {
    pub mainchain_fee: u64,
    pub height: u64,
    pub dest: [u8; 20],
    pub amount: u64,
}

/// This trait orders outputs on how "good" they are.
///
/// The most important thing is mainchain fee. Outputs with higher fees are
/// better.
///
/// The second most important thing is height. Older outputs i.e. outputs with
/// lower height are better.
///
/// And in order to make it impossible for two outputs to be equal i.e. us not
/// being able to determine which one is better, if both have the same fee and
/// height, we also compare their mainchain addresses (dest) lexicographically.
///
/// Note that when we create a bundle we aggregate all outputs by their mainchain
/// address, so when we create a bundle there are never two outputs with equal
/// mainchain addresses.
///
/// And just for completeness sake we order outputs by amount as well (which is
/// never actually used).
impl Ord for WithdrawalOutput {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.mainchain_fee == other.mainchain_fee
            && self.height == other.height
            && self.dest == other.dest
            && self.amount == other.amount
        {
            Ordering::Equal
            // Output with greater fee is better.
        } else if self.mainchain_fee > other.mainchain_fee
            // Output with lower height i.e. older output is better.
            || self.height < other.height
            || self.dest > other.dest
            || self.amount > other.amount
        {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }
}

impl PartialEq for WithdrawalOutput {
    fn eq(&self, other: &Self) -> bool {
        self.mainchain_fee == other.mainchain_fee
            && self.height == other.height
            && self.dest == other.dest
            && self.amount == other.amount
    }
}

impl PartialOrd for WithdrawalOutput {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.mainchain_fee == other.mainchain_fee
            && self.height == other.height
            && self.dest == other.dest
            && self.amount == other.amount
        {
            Some(Ordering::Equal)
        } else if self.mainchain_fee > other.mainchain_fee
            || self.height < other.height
            || self.dest > other.dest
            || self.amount > other.amount
        {
            Some(Ordering::Greater)
        } else {
            Some(Ordering::Less)
        }
    }
}
