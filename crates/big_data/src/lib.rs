extern crate core;
extern crate rustler;
extern crate serde;
extern crate ordermap;
mod atoms;
pub mod big_data;
mod nif;
mod options;
// define nif api
rustler::init!("big_data_nif", [nif::new, nif::clear], load = nif::on_load);
