use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use atoms;
use atoms::ok;
use big_data::BigData;
use big_data::RowData;
use big_data::RowTerm;
use big_data::Time;
use options::NifBigDataOptions;
use rustler::resource::ResourceArc;
use rustler::types::tuple::get_tuple;
use rustler::types::tuple::make_tuple;
use rustler::{Binary, Encoder, Env, NifResult, OwnedBinary, Term};
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
    fn update_elem(&mut self, big_key: &str, row_id: &str, elem_spec: RowTerm) -> Vec<bool> {
        if let Some(big_data) = self.data.get_mut(big_key) {
            big_data.update_elem(row_id, elem_spec)
        } else {
            vec![false]
        }
    }
    fn update_counter(&mut self, big_key: &str, row_id: &str, elem_spec: RowTerm) -> Vec<bool> {
        if let Some(big_data) = self.data.get_mut(big_key) {
            big_data.update_counter(row_id, elem_spec)
        } else {
            vec![false]
        }
    }
    // get
    fn get_row(&self, big_key: &str, row_id: &str) -> Option<&RowData> {
        if let Some(big_data) = self.get(big_key) {
            big_data.get(row_id)
        } else {
            None
        }
    }
    fn get(&self, big_key: &str) -> Option<&BigData> {
        self.data.get(big_key)
    }
    fn get_range(&self, big_key: &str, start_time: u128, end_time: u128) -> Vec<&RowData> {
        if let Some(big_data) = self.get(big_key) {
            big_data.get_range(start_time, end_time)
        } else {
            let l: Vec<&RowData> = Vec::new();
            l
        }
    }
    fn get_range_row_ids(&self, big_key: &str, start_time: u128, end_time: u128) -> Vec<&String> {
        if let Some(big_data) = self.get(big_key) {
            big_data.get_range_row_ids(start_time, end_time)
        } else {
            let l: Vec<&String> = Vec::new();
            l
        }
    }
    fn get_row_ids(&self, big_key: &str, time: u128) -> Vec<&String> {
        if let Some(big_data) = self.get(big_key) {
            big_data.get_row_ids(time)
        } else {
            let l: Vec<&String> = Vec::new();
            l
        }
    }
    fn get_time_index(&self, big_key: &str, row_id: &str) -> Option<u128> {
        if let Some(big_data) = self.get(big_key) {
            big_data.get_time_index(row_id)
        } else {
            None
        }
    }
    fn lookup_elem(&self, big_key: &str, row_id: &str, elem_spec: RowTerm) -> Vec<&RowTerm> {
        if let Some(big_data) = self.get(big_key) {
            big_data.lookup_elem(row_id, elem_spec)
        } else {
            let v: Vec<&RowTerm> = Vec::new();
            v
        }
    }
    // to_list
    fn to_list(&self, big_key: &str) -> Vec<&RowData> {
        if let Some(big_data) = self.get(big_key) {
            big_data.to_list()
        } else {
            let l: Vec<&RowData> = Vec::new();
            l
        }
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
    Ok(b.encode(env))
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
    Ok(b.encode(env))
}
#[rustler::nif]
fn get_row<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
    row_id: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let read = resource.read();
    if let Some(rs) = read.get_row(
        u8_to_string(&big_key).as_ref(),
        u8_to_string(&row_id).as_ref(),
    ) {
        Ok((*rs).encode(env))
    } else {
        Ok(atoms::notfound().encode(env))
    }
}
#[rustler::nif]
fn get<'a>(
    env: Env<'a>,
    resource: ResourceArc<NifBigDataResource>,
    big_key: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let read = resource.read();
    let list = read.to_list(u8_to_string(&big_key).as_ref());
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
    Ok((list).encode(env))
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
    Ok((list).encode(env))
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
    Ok((list).encode(env))
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
    let term = read.lookup_elem(
        u8_to_string(&big_key).as_ref(),
        u8_to_string(&row_id).as_ref(),
        elem_spec,
    );
    let terms: Vec<_> = term.into_iter().map(|t| t.encode(env)).collect();
    Ok(make_tuple(env, terms.as_ref()).encode(env))
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
use std::convert::TryFrom;
pub fn convert_to_integer(term: &Term) -> Option<u128> {
    if term.is_number() {
        match term.decode::<i64>() {
            Ok(i) => {
                let u: u128 = u128::try_from(i).unwrap();
                Some(u)
            }
            Err(_) => None,
        }
    } else {
        None
    }
}
pub fn convert_to_row_term(term: &Term) -> Option<RowTerm> {
    if term.is_number() {
        match term.decode() {
            Ok(i) => Some(RowTerm::Integer(i)),
            Err(_) => None,
        }
    } else if term.is_atom() {
        match term.atom_to_string() {
            Ok(a) => Some(RowTerm::Atom(a)),
            Err(_) => None,
        }
    } else if term.is_tuple() {
        match get_tuple(*term) {
            Ok(t) => {
                let initial_length = t.len();
                let inner_terms: Vec<RowTerm> = t
                    .into_iter()
                    .filter_map(|i: Term| convert_to_row_term(&i))
                    .collect();
                if initial_length == inner_terms.len() {
                    Some(RowTerm::Tuple(inner_terms))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    } else if term.is_list() {
        match term.decode::<Vec<Term>>() {
            Ok(l) => {
                let initial_length = l.len();
                let inner_terms: Vec<RowTerm> = l
                    .into_iter()
                    .filter_map(|i: Term| convert_to_row_term(&i))
                    .collect();
                if initial_length == inner_terms.len() {
                    Some(RowTerm::List(inner_terms))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    } else if term.is_binary() {
        match term.decode() {
            Ok(b) => Some(RowTerm::Bitstring(b)),
            Err(_) => None,
        }
    } else {
        None
    }
}
