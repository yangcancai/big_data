use anyhow::Result;
pub trait FromBytes: Sized {
    fn from_bytes(_: &[u8]) -> Result<Self>;
}
pub trait ToBytes: Sized {
    fn to_bytes(self) -> Result<Vec<u8>>;
}
