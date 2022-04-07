extern crate ordermap;

#[cfg(feature = "nif")]
mod atoms;
pub mod big_data;
#[cfg(feature = "nif")]
pub mod nif;
#[cfg(not(feature = "nif"))]
pub mod term;
pub mod traits;
