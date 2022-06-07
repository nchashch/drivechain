use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

#[derive(Eq, PartialEq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
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
        if self == other {
            Ordering::Equal
        } else if self.mainchain_fee > other.mainchain_fee
            || self.height < other.height
            || self.dest > other.dest
            || self.amount > other.amount
        {
            // Output with greater fee is better.
            Ordering::Greater
        } else {
            // Output with lower height i.e. older output is better.
            Ordering::Less
        }
    }
}

impl PartialOrd for WithdrawalOutput {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
