/// Sample Event
#[derive(
    borsh::BorshDeserialize,
    borsh::BorshSerialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    PartialEq,
    Clone,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    /// CPU Heavy Operation event
    RanCPUHeavyOperation(u64, Vec<u8>),
    /// Read and set many individual values event
    ReadAndSetManyIndividualValues(u64),
    /// Read and set heavy state event
    ReadAndSetHeavyState(u64, u64),
}
