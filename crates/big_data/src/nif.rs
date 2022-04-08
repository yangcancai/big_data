use rustler::resource::ResourceArc;
use rustler::types::binary::OwnedBinary;
use rustler::types::tuple::make_tuple;
use rustler::Binary;
use rustler::Encoder;
use rustler::Env;
use rustler::NifResult;
use rustler::Term;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use atoms;
use atoms::ok;
use core::big_data::BigData;
use core::big_data::RowData;
use core::big_data::RowTerm;
use core::big_data::Time;
use options::NifBigDataOptions;
// =================================================================================================
// resource
// =================================================================================================
struct NifBigData {
    data: HashMap<String, BigData>,
}
impl NifBigData {
    // create
    fn new(_: NifBigDataOptions) -> Result<Self, String> {
        let d = HashMap::new();
        Ok(NifBigData { data: d })
    }
    // insert
    fn insert(&mut self, big_key: &str, row_data: RowData) {
        if let Some(big_data) = self.data.get_mut(big_key) {
            big_data.insert(row_data);
        } else {
            let mut big_data = BigData::new();
            big_data.insert(row_data);
            self.data.insert(big_key.to_string(), big_data);
        }
    }
    fn append(&mut self, big_key: &str, row_data: RowData, option: RowTerm) {
        if let Some(big_data) = self.data.get_mut(big_key) {
            let _ = big_data.append(row_data, &option);
        } else {
            let mut big_data = BigData::new();
            let _ = big_data.append(row_data, &option);
            self.data.insert(big_key.to_string(), big_data);
        }
    }
    fn update_elem(
        &mut self,
        big_key: &str,
        row_id: &str,
        elem_spec: RowTerm,
    ) -> Option<Vec<bool>> {
        self.data
            .get_mut(big_key)
            .map(|big_data| big_data.update_elem(row_id, elem_spec))
    }
    fn update_counter(
        &mut self,
        big_key: &str,
        row_id: &str,
        elem_spec: RowTerm,
    ) -> Option<Vec<bool>> {
        self.data
            .get_mut(big_key)
            .map(|big_data| big_data.update_counter(row_id, elem_spec))
    }
    fn get(&self, big_key: &str) -> Option<&BigData> {
        self.data.get(big_key)
    }
    fn get_range(&self, big_key: &str, start_time: u128, end_time: u128) -> Option<Vec<&RowData>> {
        self.get(big_key)
            .map(|big_data| big_data.get_range(start_time, end_time))
    }
    fn get_range_row_ids(
        &self,
        big_key: &str,
        start_time: u128,
        end_time: u128,
    ) -> Option<Vec<&String>> {
        self.get(big_key)
            .map(|big_data| big_data.get_range_row_ids(start_time, end_time))
    }
    fn get_row_ids(&self, big_key: &str, time: u128) -> Option<Vec<&String>> {
        self.get(big_key).map(|big_data| big_data.get_row_ids(time))
    }
    fn get_time_index(&self, big_key: &str, row_id: &str) -> Option<u128> {
        if let Some(big_data) = self.get(big_key) {
            big_data.get_time_index(row_id)
        } else {
            None
        }
    }
    fn lookup_elem(
        &self,
        big_key: &str,
        row_id: &str,
        elem_spec: RowTerm,
    ) -> Option<Vec<&RowTerm>> {
        self.get(big_key)
            .map(|big_data| big_data.lookup_elem(row_id, elem_spec))
    }
    // to_list
    fn to_list(&self, big_key: &str) -> Option<Vec<&RowData>> {
        self.get(big_key).map(|big_data| big_data.to_list())
    }
    fn big_key_list(&self) -> Vec<&String> {
        let mut l = Vec::new();
        for key in self.data.keys() {
            l.push(key);
        }
        l
    }
    // clear all big_data
    fn clear(&mut self) {
        self.data.clear();
    }
    // remove a big_data
    fn remove(&mut self, big_key: &str) {
        self.data.remove(big_key);
    }
    //  remove a row
    fn remove_row(&mut self, big_key: &str, row_id: &str) {
        if let Some(big_data) = self.data.get_mut(big_key) {
            big_data.remove(row_id);
        }
    }
    // remove row_ids between start_time and end_time
    fn remove_row_ids(&mut self, big_key: &str, start_time: u128, end_time: u128) {
        if let Some(big_data) = self.data.get_mut(big_key) {
            big_data.remove_row_ids(start_time, end_time);
        }
    }
}
#[repr(transparent)]
struct NifBigDataResource(RwLock<NifBigData>);

impl NifBigDataResource {
    fn read(&self) -> RwLockReadGuard<'_, NifBigData> {
        self.0.read().unwrap()
    }

    fn write(&self) -> RwLockWriteGuard<'_, NifBigData> {
        self.0.write().unwrap()
    }
}

impl From<NifBigData> for NifBigDataResource {
    fn from(other: NifBigData) -> Self {
        NifBigDataResource(RwLock::new(other))
    }
}

pub fn on_load(env: Env, _load_info: Term) -> bool {
    rustler::resource!(NifBigDataResource, env);
    true
}
// =================================================================================================
// api
// =================================================================================================

#[rustler::nif]
fn new(env: Env, opts: NifBigDataOptions) -> NifResult<Term> {
    let rs = NifBigData::new(opts).map_err(|e| rustler::error::Error::Term(Box::new(e)))?;
    Ok((ok(), ResourceArc::new(NifBigDataResource::from(rs))).encode(env))
}
#[rustler::nif]
fn insert<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_data: RowData,
) -> NifResult<Term<'a>> {
    let mut write = resource.write();
    write.insert(u8_to_string(&big_key).as_ref(), row_data);
    Ok(ok().encode(env))
}
#[rustler::nif]
fn append<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_data: RowData,
    option: RowTerm,
) -> NifResult<Term<'a>> {
    let mut write = resource.write();
    write.append(u8_to_string(&big_key).as_ref(), row_data, option);
    Ok(ok().encode(env))
}

#[rustler::nif]
fn insert_new<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_data_list: Vec<RowData>,
) -> NifResult<Term<'a>> {
    let mut write = resource.write();
    for row in row_data_list {
        write.insert(u8_to_string(&big_key).as_ref(), row);
    }
    Ok(ok().encode(env))
}
#[rustler::nif]
fn update_elem<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_id: LazyBinary<'a>,
    elem_spec: RowTerm,
) -> NifResult<Term<'a>> {
    let mut write = resource.write();
    let b = write.update_elem(
        u8_to_string(&big_key).as_ref(),
        u8_to_string(&row_id).as_ref(),
        elem_spec,
    );
    match b {
        None => Ok(atoms::notfound().encode(env)),
        Some(b) => {
            if !b.is_empty() {
                Ok(b.encode(env))
            } else {
                Ok(atoms::notfound().encode(env))
            }
        }
    }
}
#[rustler::nif]
fn update_counter<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_id: LazyBinary<'a>,
    elem_spec: RowTerm,
) -> NifResult<Term<'a>> {
    let mut write = resource.write();
    let b = write.update_counter(
        u8_to_string(&big_key).as_ref(),
        u8_to_string(&row_id).as_ref(),
        elem_spec,
    );
    match b {
        None => Ok(atoms::notfound().encode(env)),
        Some(b) => {
            if !b.is_empty() {
                Ok(b.encode(env))
            } else {
                Ok(atoms::notfound().encode(env))
            }
        }
    }
}
#[rustler::nif]
fn get_row<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_id: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let read = resource.read();
    if let Some(big_data) = read.get(u8_to_string(&big_key).as_ref()) {
        if let Some(rs) = big_data.get(u8_to_string(&row_id).as_ref()) {
            return Ok((vec![rs]).encode(env));
        }
    }
    Ok((Vec::<&RowData>::new()).encode(env))
}
#[rustler::nif]
fn get<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let read = resource.read();
    let list = read.to_list(u8_to_string(&big_key).as_ref());
    if list == None {
        Ok((Vec::<&RowData>::new()).encode(env))
    } else {
        Ok((list).encode(env))
    }
}
#[rustler::nif]
fn big_key_list(env: Env, resource: ResourceArc<NifBigDataResource>) -> NifResult<Term> {
    let read = resource.read();
    let list = read.big_key_list();
    Ok((list).encode(env))
}
#[rustler::nif]
fn get_range<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    start_time: Time,
    end_time: Time,
) -> NifResult<Term<'a>> {
    let read = resource.read();
    let list = read.get_range(u8_to_string(&big_key).as_ref(), start_time.0, end_time.0);
    if list == None {
        Ok((Vec::<&RowData>::new()).encode(env))
    } else {
        Ok((list).encode(env))
    }
}

#[rustler::nif]
fn get_range_row_ids<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    start_time: Time,
    end_time: Time,
) -> NifResult<Term<'a>> {
    let read = resource.read();
    let list = read.get_range_row_ids(u8_to_string(&big_key).as_ref(), start_time.0, end_time.0);
    if list == None {
        Ok((Vec::<&String>::new()).encode(env))
    } else {
        Ok((list).encode(env))
    }
}
#[rustler::nif]
fn get_row_ids<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    time: Time,
) -> NifResult<Term<'a>> {
    let read = resource.read();
    let list = read.get_row_ids(u8_to_string(&big_key).as_ref(), time.0);
    if list == None {
        Ok((Vec::<&String>::new()).encode(env))
    } else {
        Ok((list).encode(env))
    }
}
#[rustler::nif]
fn get_time_index<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_id: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let read = resource.read();
    if let Some(time) = read.get_time_index(
        u8_to_string(&big_key).as_ref(),
        u8_to_string(&row_id).as_ref(),
    ) {
        let i: i64 = time as i64;
        Ok(i.encode(env))
    } else {
        Ok(atoms::notfound().encode(env))
    }
}
#[rustler::nif]
fn lookup_elem<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_id: LazyBinary<'a>,
    elem_spec: RowTerm,
) -> NifResult<Term<'a>> {
    let read = resource.read();
    if let Some(term) = read.lookup_elem(
        u8_to_string(&big_key).as_ref(),
        u8_to_string(&row_id).as_ref(),
        elem_spec,
    ) {
        let terms: Vec<_> = term.into_iter().map(|t| t.encode(env)).collect();
        Ok(make_tuple(env, terms.as_ref()).encode(env))
    } else {
        Ok(atoms::notfound().encode(env))
    }
}
#[rustler::nif]
fn clear(env: Env, resource: ResourceArc<NifBigDataResource>) -> NifResult<Term> {
    resource.write().clear();
    Ok(ok().encode(env))
}

#[rustler::nif]
fn remove<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    resource.write().remove(u8_to_string(&big_key).as_ref());
    Ok(ok().encode(env))
}
#[rustler::nif]
fn remove_row<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_id: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    resource.write().remove_row(
        u8_to_string(&big_key).as_ref(),
        u8_to_string(&row_id).as_ref(),
    );
    Ok(ok().encode(env))
}

#[rustler::nif]
fn remove_row_ids<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    start_time: Time,
    end_time: Time,
) -> NifResult<Term<'a>> {
    resource
        .write()
        .remove_row_ids(u8_to_string(&big_key).as_ref(), start_time.0, end_time.0);
    Ok(ok().encode(env))
}
// =================================================================================================
// helpers
// =================================================================================================

/// Represents either a borrowed `Binary` or `OwnedBinary`.
///
/// `LazyBinary` allows for the most efficient conversion from an
/// Erlang term to a byte slice. If the term is an actual Erlang
/// binary, constructing `LazyBinary` is essentially
/// zero-cost. However, if the term is any other Erlang type, it is
/// converted to an `OwnedBinary`, which requires a heap allocation.
enum LazyBinary<'a> {
    Owned(OwnedBinary),
    Borrowed(Binary<'a>),
}

impl<'a> std::ops::Deref for LazyBinary<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Owned(owned) => owned.as_ref(),
            Self::Borrowed(borrowed) => borrowed.as_ref(),
        }
    }
}

impl<'a> rustler::Decoder<'a> for LazyBinary<'a> {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if term.is_binary() {
            Ok(Self::Borrowed(Binary::from_term(term)?))
        } else {
            Ok(Self::Owned(term.to_binary()))
        }
    }
}
fn u8_to_string(msg: &[u8]) -> String {
    let a = String::from_utf8_lossy(msg);
    match a {
        Cow::Owned(own_msg) => own_msg,
        Cow::Borrowed(b_msg) => b_msg.to_string(),
    }
}
