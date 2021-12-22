extern crate core;
extern crate rustler;
extern crate serde;
mod atoms;
mod nif;
mod options;
// define nif api
rustler::init!(
    "big_data_nif",
    [
        nif::new,
        nif::clear,
        nif::insert,
        nif::insert_new,
        nif::update_elem,
        nif::update_counter,
        nif::get_row,
        nif::get,
        nif::get_range,
        nif::get_range_row_ids,
        nif::get_row_ids,
        nif::get_time_index,
        nif::lookup_elem,
        nif::remove,
        nif::remove_row,
        nif::remove_row_ids,
        nif::big_key_list,
    ],
    load = nif::on_load
);
