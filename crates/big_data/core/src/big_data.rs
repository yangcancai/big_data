use super::atoms;
use ordermap::set::OrderSet;
use rustler::dynamic::get_type;
use rustler::error::Error;
use rustler::types::atom::Atom;
use rustler::types::binary::OwnedBinary;
use rustler::types::map::map_new;
use rustler::types::tuple::get_tuple;
use rustler::types::tuple::make_tuple;
use rustler::Decoder;
use rustler::Encoder;
use rustler::Env;
use rustler::MapIterator;
use rustler::NifResult;
use rustler::Term;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Bound::Included;
pub struct Time(pub u128);
#[derive(Debug, Clone)]
pub struct RowData {
    // id
    pub row_id: String,
    // RowTerm
    pub term: RowTerm,
    // timestamp
    pub time: u128,
}
#[derive(Debug, Clone)]
pub struct BigData {
    // RowId  RowTerm
    rows: HashMap<String, RowData>,
    // BTreeMap to range sort rows
    // The same time may be have mutiple RowId
    time_index: BTreeMap<u128, Box<OrderSet<String>>>,
}
/// RowTerm is an enum that covers all the Erlang / Elixir term types that can be stored in
/// a BigData.
///
/// There are a number of types that are not supported because of their complexity and the
/// difficulty of safely implementing their storage.
///
/// Types that are not supported
///   - Reference
///   - Function
///   - Port
///   - Pid
///
/// Types that are supported but not explicitly listed
///   - Boolean (Note that booleans in Erlang / Elixir are just atoms)
#[derive(Debug, Clone)]
pub enum RowTerm {
    Integer(i64),
    Atom(String),
    Tuple(Vec<RowTerm>),
    List(Vec<RowTerm>),
    Bitstring(String),
    Bin(Vec<u8>),
    Float(f64),
    Map(Vec<(RowTerm, RowTerm)>),
}
impl RowData {
    pub fn new(row_id: &str, row_term: RowTerm, time: u128) -> Self {
        RowData {
            row_id: row_id.to_string(),
            term: row_term,
            time,
        }
    }
}
impl PartialEq for RowData {
    fn eq(&self, other: &RowData) -> bool {
        if self.row_id == other.row_id && self.term == other.term {
            return self.time == other.time;
        }
        false
    }
}
impl Default for BigData {
    fn default() -> Self {
        BigData::new()
    }
}
impl BigData {
    pub fn new() -> Self {
        BigData {
            rows: HashMap::new(),
            time_index: BTreeMap::new(),
        }
    }
    /// Insert RowData to the BigData
    ///  
    /// # Examples
    /// ```
    ///    use core::big_data::BigData;
    ///    use core::big_data::RowTerm;
    ///    use core::big_data::RowData;
    ///    let mut big_data = BigData::new();
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let insert_result = big_data.insert(RowData::new("1", r.clone(), 1234567890));
    ///    assert_eq!(None, insert_result);
    /// ```
    ///
    pub fn insert(&mut self, row_data: RowData) -> Option<RowData> {
        let time = row_data.time;
        let row_id = row_data.row_id.clone();
        let rs = match self.rows.insert(row_id.clone(), row_data) {
            // first insert
            None => None,
            // second insert
            Some(r) => {
                let old_time = r.time;
                // clear old time_index
                if let Some(set) = self.time_index.get_mut(&old_time) {
                    set.remove(&row_id);
                }
                Some(r)
            }
        };
        if let Some(set) = self.time_index.get_mut(&time) {
            set.insert(row_id);
        } else {
            let mut set = OrderSet::new();
            set.insert(row_id);
            self.time_index.insert(time, Box::new(set));
        }
        rs
    }
    pub fn insert_list(&mut self, list: Vec<RowData>) -> Option<RowData> {
        let mut rs = None;
        for row in list {
            rs = self.insert(row);
        }
        rs
    }
    /// Clear all RowData and all time_index
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// use core::big_data::{BigData, RowData, RowTerm};
    /// let mut big_data = BigData::new();
    /// big_data.clear();
    /// insert_data(&mut big_data, "a", &1);
    /// big_data.clear();
    /// assert_eq!(None, big_data.get("a"));
    /// let mut ids:Vec<&str> = vec![];
    /// assert_eq!(ids, big_data.get_row_ids(1));
    /// assert_eq!(None, big_data.get_time_index("a"));
    /// fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128){
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(row_id, r.clone(), *time);
    ///    big_data.insert(row1);
    /// }
    /// ```
    pub fn clear(&mut self) {
        self.time_index.clear();
        self.rows.clear();
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn len_time_index(&self) -> usize {
        self.time_index.len()
    }
    /// Length of row_ids by time
    /// maybe the same time will return great than 1
    ///
    /// # Examples
    ///
    ///
    /// ```
    ///
    /// use core::big_data::{BigData, RowData, RowTerm};
    ///  let mut big_data = BigData::new();
    ///  insert_data(&mut big_data, "a", &0);
    ///  insert_data(&mut big_data, "b", &1);
    ///  assert_eq!(2, big_data.len());
    ///  assert_eq!(1, big_data.len_row_ids(0));
    ///  assert_eq!(1, big_data.len_row_ids(1));
    ///  insert_data(&mut big_data, "b", &0);
    ///  assert_eq!(2, big_data.len_row_ids(0));
    ///  assert_eq!(0, big_data.len_row_ids(1));
    /// fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128){
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(row_id, r.clone(), *time);
    ///    big_data.insert(row1);
    /// }
    ///
    /// ```
    pub fn len_row_ids(&self, time: u128) -> usize {
        let list = self.get_row_ids(time);
        list.len()
    }
    /// Lenght of row_ids between start_time and end_time (Included(start_time), Included(end_time))
    ///
    /// # Examples
    ///
    /// ```        
    /// use core::big_data::{BigData, RowData, RowTerm};
    ///  let mut big_data = BigData::new();
    ///  insert_data(&mut big_data, "a", &0);
    ///  insert_data(&mut big_data, "b", &1);
    ///  assert_eq!(2, big_data.len_range_row_ids(0, 1));
    ///  insert_data(&mut big_data, "b", &0);
    ///  assert_eq!(2, big_data.len_range_row_ids(0, 0));
    ///  insert_data(&mut big_data, "c", &5);
    ///  assert_eq!(2, big_data.len_range_row_ids(0, 4));
    ///  assert_eq!(3, big_data.len_range_row_ids(0, 5));
    /// fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128){
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(row_id, r.clone(), *time);
    ///    big_data.insert(row1);
    /// }

    ///
    /// ```
    pub fn len_range_row_ids(&self, start_time: u128, end_time: u128) -> usize {
        let list = self.get_range_row_ids(start_time, end_time);
        list.len()
    }
    fn add(a: &mut RowTerm, b: &RowTerm) -> bool {
        match a.clone().add(b.clone()) {
            (new, false) => {
                *a = new;
                true
            }
            (_, _) => false,
        }
    }
    /// Update Counter for element
    ///
    /// # Examples
    ///
    ///   Please see below test fn update_counter
    ///
    pub fn update_counter(&mut self, row_id: &str, elem_spec: RowTerm) -> Vec<bool> {
        match elem_spec {
            // elem_spec = {pos, incr}
            RowTerm::Tuple(tuple) => {
                if let Some(row_data) = self.rows.get_mut(row_id) {
                    match &mut row_data.term {
                        // tuple
                        RowTerm::Tuple(row_data_tuple) => match tuple[0] {
                            RowTerm::Integer(i) => {
                                if i >= 0 && row_data_tuple.len() as i64 > i {
                                    return vec![Self::add(
                                        &mut row_data_tuple[i as usize],
                                        &tuple[1],
                                    )];
                                } else {
                                    return vec![false];
                                }
                            }
                            _ => return vec![false],
                        },
                        // list
                        RowTerm::List(row_data_list) => {
                            if let RowTerm::Integer(i) = tuple[0] {
                                if row_data_list.len() as i64 > i && i >= 0 {
                                    return vec![Self::add(
                                        &mut row_data_list[i as usize],
                                        &tuple[1],
                                    )];
                                } else {
                                    return vec![false];
                                }
                            } else {
                                return vec![false];
                            }
                        }
                        // just single element
                        _ => {
                            if RowTerm::Integer(0) == tuple[0] {
                                return vec![Self::add(&mut row_data.term, &tuple[1])];
                            } else {
                                return vec![false];
                            }
                        }
                    }
                } else {
                    return vec![false];
                }
            }
            // elem_spec = [{pos, elem}, ...]
            RowTerm::List(list) => {
                let mut rs: Vec<bool> = Vec::new();
                for pos_elem in list {
                    let b = self.update_counter(row_id, pos_elem);
                    rs.push(b[0]);
                }
                rs
            }
            _ => {
                return vec![false];
            }
        }
    }
    /// Update element
    ///
    /// # Examples
    ///
    /// Please see below test fn update_elem
    ///
    pub fn update_elem(&mut self, row_id: &str, elem_spec: RowTerm) -> Vec<bool> {
        match elem_spec {
            // elem_spec = {pos, elem}
            RowTerm::Tuple(tuple) => {
                if let Some(row_data) = self.rows.get_mut(row_id) {
                    match &mut row_data.term {
                        // tuple
                        RowTerm::Tuple(row_data_tuple) => {
                            if let RowTerm::Integer(i) = tuple[0] {
                                if row_data_tuple.len() as i64 > i && i >= 0 {
                                    row_data_tuple[i as usize] = tuple[1].clone();
                                    return vec![true];
                                } else {
                                    return vec![false];
                                }
                            } else {
                                return vec![false];
                            }
                        }
                        // list
                        RowTerm::List(row_data_list) => {
                            if let RowTerm::Integer(i) = tuple[0] {
                                if row_data_list.len() as i64 > i && i >= 0 {
                                    row_data_list[i as usize] = tuple[1].clone();
                                    return vec![true];
                                } else {
                                    return vec![false];
                                }
                            } else {
                                return vec![false];
                            }
                        }
                        // just single element
                        _ => {
                            if RowTerm::Integer(0) == tuple[0] {
                                row_data.term = tuple[1].clone();
                                return vec![true];
                            } else {
                                return vec![false];
                            }
                        }
                    }
                } else {
                    return vec![false];
                }
            }
            // elem_spec = [{pos, elem}, ...]
            RowTerm::List(list) => {
                let mut rs: Vec<bool> = Vec::new();
                for pos_elem in list {
                    let vec_bool = self.update_elem(row_id, pos_elem);
                    rs.push(vec_bool[0]);
                }
                rs
            }
            _ => {
                return vec![false];
            }
        }
    }
    pub fn lookup_elem(&self, row_id: &str, elem_spec: RowTerm) -> Vec<&RowTerm> {
        match elem_spec {
            RowTerm::Integer(pos) => {
                if let Some(row_data) = self.rows.get(row_id) {
                    match &row_data.term {
                        // tuple or list
                        RowTerm::List(row_data_tuple) | RowTerm::Tuple(row_data_tuple) => {
                            if row_data_tuple.len() as i64 > pos && pos >= 0 {
                                return vec![&row_data_tuple[pos as usize]];
                            }
                        }
                        // a single elem
                        single => {
                            if pos == 0 {
                                return vec![single];
                            }
                        }
                    }
                }
            }
            RowTerm::Tuple(list) | RowTerm::List(list) => {
                let mut rs: Vec<&RowTerm> = Vec::new();
                for pos_elem in list {
                    let vec = self.lookup_elem(row_id, pos_elem);
                    if !vec.is_empty() {
                        rs.push(vec[0]);
                    }
                }
                return rs;
            }
            RowTerm::Bitstring(str) => {
                let vec = str.into_bytes();
                let mut list = vec![];
                for row in vec {
                    list.push(RowTerm::Integer(row as i64));
                }
                return self.lookup_elem(row_id, RowTerm::List(list));
            }
            _ => {}
        }
        let l: Vec<&RowTerm> = Vec::new();
        l
    }
    /// Get a RowData From BigData, such as a key value
    ///
    /// # Examples
    ///
    /// ```
    ///     use core::big_data::{BigData,RowTerm,RowData};
    ///    let mut big_data = BigData::new();
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new("a", r.clone(), 1234567890);
    ///    assert_eq!(None, big_data.get("a"));
    ///    big_data.insert(row1.clone());
    ///    let row = big_data.get("a");
    ///    assert_eq!(row, Some(&row1))
    ///
    /// ```
    pub fn get(&self, row_id: &str) -> Option<&RowData> {
        self.rows.get(row_id)
    }
    /// Get timestamp by row_id
    ///
    /// # Examples
    ///
    /// ```
    ///     use core::big_data::{RowTerm,RowData,BigData};
    ///    let mut big_data = BigData::new();
    ///    assert_eq!(None, big_data.get_time_index("a"));
    ///    insert_data(&mut big_data, "a", &1);
    ///    insert_data(&mut big_data, "b", &2);
    ///    assert_eq!(Some(1), big_data.get_time_index("a"));
    ///    assert_eq!(Some(2), big_data.get_time_index("b"));
    ///
    /// fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128){
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(row_id,r.clone(), *time);
    ///    big_data.insert(row1);
    /// }
    /// ```
    ///
    ///
    pub fn get_time_index(&self, row_id: &str) -> Option<u128> {
        self.get(row_id).map(|row| row.time)
    }
    /// Get row_ids by timestamp
    ///
    /// # Examples
    ///
    /// ```
    /// use core::big_data::{BigData,RowTerm,RowData};
    /// let mut big_data = BigData::new();
    /// let ids: Vec<&String> = Vec::new();
    /// assert_eq!(ids, big_data.get_row_ids(1));
    /// insert_data(&mut big_data, "a", &1);
    /// let mut ids = vec!["a"];
    /// assert_eq!(ids, big_data.get_row_ids(1));
    /// insert_data(&mut big_data, "b", &1);
    /// ids.push("b");
    /// assert_eq!(ids, big_data.get_row_ids(1));
    /// fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128){
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(row_id,r.clone(), *time);
    ///    big_data.insert(row1);
    /// }
    /// ```
    ///
    pub fn get_row_ids(&self, time: u128) -> Vec<&String> {
        match self.time_index.get(&time) {
            None => Vec::new(),
            Some(set) => {
                let mut l = Vec::new();
                for row_id in set.as_ref() {
                    l.push(row_id);
                }
                l
            }
        }
    }
    /// Query RowData from start_time to end_time included start_time and included end_time
    ///
    ///
    /// # Examples
    ///
    /// ```
    /// use core::big_data::{RowData,RowTerm,BigData};
    /// let mut big_data = BigData::new();
    ///  // get range RowData
    ///  let vec_data1: Vec<&RowData> = Vec::new();
    ///  assert_eq!(vec_data1, big_data.get_range(0, 99999999999999));
    ///  let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///  let row1 = RowData::new("a", r, 1234567890);
    ///  big_data.insert(row1.clone());
    ///  let vec_data = big_data.get_range(0, 1234567890);
    ///  let vec_data1 = vec![&row1];
    ///  assert_eq!(vec_data1, vec_data.clone());
    ///
    /// ```
    pub fn get_range(&self, start_time: u128, end_time: u128) -> Vec<&RowData> {
        let mut r = Vec::new();
        for v in self.get_range_row_ids(start_time, end_time) {
            if let Some(row) = self.get(v) {
                r.push(row);
            }
        }
        r
    }
    /// Get Range RowIds by timestamp range , included start_time and included end_time
    ///
    /// # Examples
    ///
    /// ```
    ///  use core::big_data::{BigData, RowData, RowTerm};
    ///  let mut big_data = BigData::new();
    ///  let mut ids: Vec<&str> = Vec::new();
    ///  assert_eq!(ids, big_data.get_range_row_ids(0,1));
    ///  insert_data(&mut big_data, "a", &0);
    ///  ids.push("a");
    ///  assert_eq!(ids, big_data.get_range_row_ids(0,1));
    ///  insert_data(&mut big_data, "b", &0);
    ///  ids.push("b");
    ///  assert_eq!(ids, big_data.get_range_row_ids(0,1));
    ///
    /// fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128){
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(row_id, r.clone(), *time);
    ///    big_data.insert(row1);
    /// }
    /// ```
    ///
    pub fn get_range_row_ids(&self, start_time: u128, end_time: u128) -> Vec<&String> {
        let mut r = Vec::new();
        for (_, value) in self
            .time_index
            .range((Included(&start_time), Included(&end_time)))
        {
            for v in value.as_ref() {
                r.push(v);
            }
        }
        r
    }
    /// BigData to list, sort by time
    ///
    /// # Examples
    ///
    /// ```
    /// use core::big_data::{BigData, RowData, RowTerm};
    /// let mut big_data = BigData::new();
    /// let d = RowData::new("a", RowTerm::Integer(0), 10);
    /// big_data.insert(d.clone());
    /// let mut list = Vec::new();
    /// list.push(&d);
    /// assert_eq!(list, big_data.to_list());
    /// let e = RowData::new("b", RowTerm::Integer(0), 1);
    /// big_data.insert(e.clone());
    /// list.clear();
    /// list.push(&e);
    /// list.push(&d);
    /// assert_eq!(list, big_data.to_list());
    /// ```
    pub fn to_list(&self) -> Vec<&RowData> {
        let mut list = Vec::new();
        for (_, set) in self.time_index.iter() {
            for id in set.as_ref() {
                if let Some(v) = self.rows.get(id) {
                    list.push(v)
                }
            }
        }
        list
    }
    /// Remove RowId
    ///
    /// # Examples
    ///
    /// ```
    ///
    ///   use core::big_data::{BigData, RowData, RowTerm};
    ///   let mut big_data = BigData::new();
    ///    big_data.remove("0");
    ///    insert_data(&mut big_data, "a", &1);
    ///    assert_eq!(Some(1), big_data.get_time_index("a"));
    ///    big_data.remove("a");
    ///    assert_eq!(None, big_data.get_time_index("a"));
    ///    assert_eq!(None, big_data.get("a"));
    ///    let ids: Vec<&str> = Vec::new();
    ///    assert_eq!(ids, big_data.get_row_ids(1));
    /// fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128){
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(row_id, r.clone(), *time);
    ///    big_data.insert(row1);
    /// }
    /// ```
    pub fn remove(&mut self, row_id: &str) {
        // remove time_index
        match self.get_time_index(row_id) {
            None => {}
            Some(time) => {
                if let Some(set) = self.time_index.get_mut(&time) {
                    set.remove(row_id);
                }
                // remove RowData
                self.rows.remove(row_id);
            }
        }
    }
    /// Remove RowIds , Included start_time to Included end_time
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// use core::big_data::{BigData, RowData, RowTerm};
    /// let mut big_data = BigData::new();
    /// big_data.remove_row_ids(0, 10);
    /// insert_data(&mut big_data, "0", &11);
    /// insert_data(&mut big_data, "1", &1);
    /// let mut ids: Vec<&str> = Vec::new();
    /// ids.push("1");
    /// ids.push("0");
    /// assert_eq!(ids ,big_data.get_range_row_ids(0, 11));
    /// big_data.remove_row_ids(0, 10);
    /// ids.remove(0);
    /// assert_eq!(ids, big_data.get_range_row_ids(0, 11));
    /// big_data.remove_row_ids(11, 11);
    /// ids.remove(0);
    /// assert_eq!(ids, big_data.get_range_row_ids(0, 11));
    /// fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128){
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(row_id, r.clone(), *time);
    ///    big_data.insert(row1);
    /// }
    /// ```
    pub fn remove_row_ids(&mut self, start_time: u128, end_time: u128) {
        let mut time_indexs: Vec<u128> = Vec::new();
        for (time, value) in self
            .time_index
            .range((Included(&start_time), Included(&end_time)))
        {
            for row_id in value.as_ref() {
                self.rows.remove(row_id);
            }
            time_indexs.push(*time);
        }
        for time in time_indexs {
            self.time_index.remove(&time);
        }
    }
}
impl RowTerm {
    pub fn is_integer(&self) -> bool {
        matches!(self, RowTerm::Integer(_))
    }
}
fn int_overflowing_add(a: i64, b: i64, default: RowTerm) -> (RowTerm, bool) {
    let (c, overflow) = a.overflowing_add(b);
    if overflow {
        (default, overflow)
    } else {
        (RowTerm::Integer(c), overflow)
    }
}
fn float_overflowing_add(a: f64, b: f64, _default: RowTerm) -> (RowTerm, bool) {
    let c = a + b;
    //
    if c.is_finite() {
        (RowTerm::Float(c), false)
    } else {
        (RowTerm::Float(a), true)
    }
}
impl Add for RowTerm {
    type Output = (Self, bool);
    fn add(self, other: Self) -> Self::Output {
        match self {
            RowTerm::Integer(inner) => match other {
                RowTerm::Integer(other_inner) => int_overflowing_add(inner, other_inner, self),
                RowTerm::Float(other_inner) => {
                    float_overflowing_add(inner as f64, other_inner, self)
                }
                _ => (self, false),
            },
            RowTerm::Float(inner) => match other {
                RowTerm::Float(other_inner) => float_overflowing_add(inner, other_inner, self),
                RowTerm::Integer(other_inner) => {
                    float_overflowing_add(inner, other_inner as f64, self)
                }
                _ => (self, false),
            },
            r => (r, false),
        }
    }
}
impl AddAssign for RowTerm {
    fn add_assign(&mut self, other: Self) {
        let new = self.clone().add(other);
        match new {
            (new, false) => *self = new,
            (_new, true) => {}
        }
    }
}

impl PartialEq for RowTerm {
    fn eq(&self, other: &RowTerm) -> bool {
        match self {
            RowTerm::Integer(self_inner) => match other {
                RowTerm::Integer(inner) => self_inner == inner,
                _ => false,
            },
            RowTerm::Float(self_inner) => match other {
                RowTerm::Float(inner) => self_inner == inner,
                _ => false,
            },
            RowTerm::Atom(self_inner) => match other {
                RowTerm::Atom(inner) => self_inner == inner,
                _ => false,
            },
            RowTerm::Tuple(self_inner) => match other {
                RowTerm::Tuple(inner) => {
                    let length = self_inner.len();

                    if length != inner.len() {
                        return false;
                    }

                    let mut idx = 0;

                    while idx < length {
                        if self_inner[idx] != inner[idx] {
                            return false;
                        }
                        idx += 1;
                    }

                    true
                }
                _ => false,
            },
            RowTerm::List(self_inner) => match other {
                RowTerm::List(inner) => {
                    let length = self_inner.len();

                    if length != inner.len() {
                        return false;
                    }

                    let mut idx = 0;

                    while idx < length {
                        if self_inner[idx] != inner[idx] {
                            return false;
                        }
                        idx += 1;
                    }

                    true
                }
                _ => false,
            },
            RowTerm::Bitstring(self_inner) => match other {
                RowTerm::Bitstring(inner) => self_inner == inner,
                _ => false,
            },
            RowTerm::Bin(self_inner) => match other {
                RowTerm::Bin(inner) => self_inner == inner,
                _ => false,
            },
            RowTerm::Map(self_inner) => match other {
                RowTerm::Map(inner) => self_inner == inner,
                _ => false,
            },
        }
    }
}

impl Encoder for RowTerm {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            RowTerm::Integer(inner) => inner.encode(env),
            RowTerm::Atom(inner) => match Atom::from_str(env, inner) {
                Ok(atom) => atom.encode(env),
                Err(_) => atoms::error().encode(env),
            },
            RowTerm::Tuple(inner) => {
                let terms: Vec<_> = inner.iter().map(|t| t.encode(env)).collect();
                make_tuple(env, terms.as_ref()).encode(env)
            }
            RowTerm::List(inner) => inner.encode(env),
            RowTerm::Bitstring(inner) => inner.encode(env),
            RowTerm::Bin(inner) => {
                let mut binary = OwnedBinary::new(inner.len()).unwrap();
                binary.as_mut_slice().write_all(inner).unwrap();
                binary.release(env).encode(env)
            }
            RowTerm::Float(inner) => inner.encode(env),
            RowTerm::Map(map) => {
                let rs = map.iter().fold(map_new(env), |rs, (k, v)| {
                    let rs = rs.map_put(k.encode(env), v.encode(env));
                    rs.unwrap()
                });
                rs.encode(env)
            }
        }
    }
}
impl<'a> Decoder<'a> for RowTerm {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if let Some(v) = convert_to_row_term(&term) {
            Ok(v)
        } else {
            Err(Error::BadArg)
        }
    }
}
impl Encoder for RowData {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let row_data = RowTerm::Tuple(vec![
            RowTerm::Atom("row_data".to_string()),
            RowTerm::Bitstring(self.row_id.clone()),
            self.term.clone(),
            RowTerm::Integer(self.time as i64),
        ]);
        row_data.encode(env)
    }
}
impl<'a> Decoder<'a> for Time {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if let Some(i) = convert_to_integer(&term) {
            Ok(Time(i))
        } else {
            Err(Error::BadArg)
        }
    }
}

impl<'a> Decoder<'a> for RowData {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if let Some(RowTerm::Tuple(tuple)) = convert_to_row_term(&term) {
            if tuple.len() == 4
                && tuple[0] == RowTerm::Atom("row_data".to_string())
                && tuple[3].is_integer()
            {
                if let RowTerm::Integer(time) = tuple[3] {
                    match &tuple[1] {
                        RowTerm::Integer(row_id) => {
                            let row_data =
                                RowData::new(&row_id.to_string(), tuple[2].clone(), time as u128);
                            return Ok(row_data);
                        }
                        RowTerm::Bitstring(row_id) => {
                            let row_data = RowData::new(row_id, tuple[2].clone(), time as u128);
                            return Ok(row_data);
                        }
                        _ => {}
                    }
                }
            }
        }
        Err(Error::BadArg)
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
        match term.decode::<i64>() {
            Ok(i) => Some(RowTerm::Integer(i)),
            Err(_e) => match term.decode::<f64>() {
                Ok(i) => Some(RowTerm::Float(i)),
                Err(_e) => None,
            },
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
            Err(_) => match term.decode_as_binary() {
                Ok(bb) => Some(RowTerm::Bin(bb.to_vec())),
                Err(_) => None,
            },
        }
    } else if term.is_map() {
        match term.decode::<MapIterator>() {
            Ok(l) => {
                let mut rs = Vec::new();
                let mut n = 0;
                for (k, v) in l {
                    let k = convert_to_row_term(&k);
                    let v = convert_to_row_term(&v);
                    n += 1;
                    if let (Some(k), Some(v)) = (k, v) {
                        rs.push((k, v));
                    }
                }
                if n == rs.len() {
                    Some(RowTerm::Map(rs))
                } else {
                    None
                }
            }
            Err(_e) => None,
        }
    } else {
        println!("unknown type = {:?}", get_type(*term));
        None
    }
}
