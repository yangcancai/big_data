extern crate ordermap;

#[cfg(feature = "nif")]
mod atoms;
pub mod big_data;
#[cfg(not(feature = "nif"))]
pub mod term;
pub mod traits;
#[cfg(feature = "nif")]
pub mod nif;