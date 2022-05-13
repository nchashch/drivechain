#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WithdrawalOutput {
    pub dest: [u8;20],
    pub amount: u64,
    pub mainchain_fee: u64,
}
