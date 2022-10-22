use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};

/// A withdrawal request.
#[derive(Eq, PartialEq, Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct Withdrawal {
    /// Fee that is added to the "bundle fee" that will be paid to mainchain
    /// miners when the bundle is included. First and foremost withdrawals are
    /// sorted by fee, so if there are more withdrawals than available space in
    /// the next bundle can make sure that your withdrawal is included by paying
    /// a higher fee.
    pub mainchain_fee: u64,
    /// "Sidechain height" at which this withdrawal was included. Note that this
    /// is a sort of "virtual height" internal to this library and it does not
    /// depend on sidechain implementation. It is used to sort withdrawals by
    /// age. If two withdrawals have the same fee the older one will be included
    /// first.
    pub height: u64,
    /// Mainchain destination - 20 script hash bytes extracted from a mainchain
    /// address. If this withdrawal is included in a bundle the constructed
    /// mainchain bundle transaction will pay the `amount` to this address.
    pub dest: [u8; 20],
    /// Amount to be withdrawn from the sidechain and paid out in the bundle
    /// transaction on mainchain (if this withdrawal is included in the
    /// constructed bundle).
    pub amount: u64,
}

/// This trait orders outputs on how "good" they are. The most important thing
/// is mainchain fee. Outputs with higher fees are better. The second most
/// important thing is height. Older outputs i.e. outputs with lower height are
/// better.
///
/// In order to make it impossible for two outputs to be equal i.e. us not being
/// able to determine which one is better, if both have the same fee and height,
/// we also compare their mainchain addresses (dest) lexicographically.
///
/// Note that when we create a bundle we aggregate all outputs by their mainchain
/// address, so when we create a bundle there are never two outputs with equal
/// mainchain addresses.
///
/// Just for completeness sake we order outputs by amount as well (which is
/// never actually used).
impl Ord for Withdrawal {
    fn cmp(&self, other: &Self) -> Ordering {
        if self == other {
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

impl PartialOrd for Withdrawal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
