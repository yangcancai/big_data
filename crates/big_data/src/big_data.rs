use atoms;
use nif::convert_to_integer;
use nif::convert_to_row_term;
use ordermap::set::OrderSet;
use rustler::error::Error;
use rustler::types::atom::Atom;
use rustler::types::tuple::make_tuple;
use rustler::Decoder;
use rustler::Encoder;
use rustler::Env;
use rustler::NifResult;
use rustler::Term;
use std::collections::{BTreeMap, HashMap};
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Bound::Included;

pub struct Time(pub u128);
#[derive(Debug, Clone)]
pub struct RowData {
    // id
    row_id: String,
    // RowTerm
    term: RowTerm,
    // timestamp
    time: u128,
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
    /// Clear all RowData and all time_index
    ///
    /// # Examples
    ///
    /// ```
    ///
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
    ///    let row1 = RowData::new(r.clone(), *time);
    ///    big_data.insert(row_id, row1);
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
    ///  let mut big_data = BigData::new();
    ///  insert_data(&mut big_data, "a", &0);
    ///  insert_data(&mut big_data, "b", &1);
    ///  assert_eq!(2, big_data.len());
    ///  assert_eq!(1, big_data.len_row_ids(0));
    ///  assert_eq!(1, big_data.len_row_ids(1));
    ///  insert_data(&mut big_data, "b", &0);
    ///  assert_eq!(2, big_data.len_row_ids(0));
    ///  assert_eq!(0, big_data.len_row_ids(1///  
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
    ///  let mut big_data = BigData::new();
    ///  insert_data(&mut big_data, "a", &0);
    ///  insert_data(&mut big_data, "b", &1);
    ///  assert_eq!(2, big_data.len_range_row_ids(0, 1));
    ///  insert_data(&mut big_data, "b", &0);
    ///  assert_eq!(2, big_data.len_range_row_ids(0, 0));
    ///  insert_data(&mut big_data, "c", &5);
    ///  assert_eq!(2, big_data.len_range_row_ids(0, 4));
    ///  assert_eq!(3, big_data.len_range_row_ids(0, 5));
    ///
    /// ```
    pub fn len_range_row_ids(&self, start_time: u128, end_time: u128) -> usize {
        let list = self.get_range_row_ids(start_time, end_time);
        list.len()
    }
    /// Insert RowData to the BigData
    ///  
    /// # Examples
    /// ```
    ///    let mut big_data = BigData::new();
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let insert_result = big_data.insert("a", RowData::new(r.clone(), 1234567890));
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
                        RowTerm::Tuple(row_data_tuple) => {
                            if let RowTerm::Integer(i) = tuple[0] {
                                if i >= 0 && row_data_tuple.len() as i64 > i {
                                    row_data_tuple[i as usize] += tuple[1].clone();
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
                                    row_data_list[i as usize] += tuple[1].clone();
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
                                row_data.term += tuple[1].clone();
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
    ///    let mut big_data = BigData::new();
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(r.clone(), 1234567890);
    ///    assert_eq!(None, big_data.get("a"));
    ///    big_data.insert("a", row1.clone());
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
    ///    let mut big_data = BigData::new();
    ///    assert_eq!(None, big_data.get_time_index("a"));
    ///    insert_data(&mut big_data, "a", &1);
    ///    insert_data(&mut big_data, "b", &2);
    ///    assert_eq!(Some(1), big_data.get_time_index("a"));
    ///    assert_eq!(Some(2), big_data.get_time_index("b"))
    ///
    /// fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128){
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let row1 = RowData::new(r.clone(), *time);
    ///    big_data.insert(row_id, row1);
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
    ///    let row1 = RowData::new(r.clone(), *time);
    ///    big_data.insert(row_id, row1);
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
    /// let mut big_data = BigData::new();
    ///  // get range RowData
    ///  let vec_data1: Vec<&RowData> = Vec::new();
    ///  assert_eq!(vec_data1, big_data.get_range(0, 99999999999999));
    ///  let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///  let row1 = RowData::new(r, 1234567890);
    ///  big_data.insert("a", row1.clone());
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
    ///    let row1 = RowData::new(r.clone(), *time);
    ///    big_data.insert(row_id, row1);
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
    /// let mut big_data = BigData::new();
    /// let d = RowData::new(RowTerm::Integer(0), 10);
    /// big_data.insert("a",d.clone());
    /// let mut list = Vec::new();
    /// list.push(&d);
    /// assert_eq!(list, big_data.to_list());
    /// let e = RowData::new(RowTerm::Integer(0), 1);
    /// big_data.insert("b",e.clone());
    /// list.clear();
    /// list.push(&e);
    /// list.push(&d);
    /// assert_eq!(list, big_data.to_list(///
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
    ///    let row1 = RowData::new(r.clone(), *time);
    ///    big_data.insert(row_id, row1);
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
    ///    let row1 = RowData::new(r.clone(), *time);
    ///    big_data.insert(row_id, row1);
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
    fn is_integer(&self) -> bool {
        matches!(self, RowTerm::Integer(_))
    }
}
impl Add for RowTerm {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        match self {
            RowTerm::Integer(self_inner) => {
                if let RowTerm::Integer(other_inner) = other {
                    RowTerm::Integer(other_inner + self_inner)
                } else {
                    self
                }
            }
            r => r,
        }
    }
}
impl AddAssign for RowTerm {
    fn add_assign(&mut self, other: Self) {
        let new = self.clone().add(other);
        *self = new;
    }
}

impl PartialEq for RowTerm {
    fn eq(&self, other: &RowTerm) -> bool {
        match self {
            RowTerm::Integer(self_inner) => match other {
                RowTerm::Integer(inner) => self_inner == inner,
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
                                    let row_data = RowData::new(
                                        &row_id.to_string(),
                                        tuple[2].clone(),
                                        time as u128,
                                    );
                                    return Ok(row_data);
                                }
                                RowTerm::Bitstring(row_id) => {
                                    let row_data =
                                        RowData::new(&row_id, tuple[2].clone(), time as u128);
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

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn insert() {
        let mut big_data = BigData::new();
        let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
        // first insert
        let insert_result = big_data.insert(RowData::new("a", r.clone(), 1234567890));
        assert_eq!(Some(1234567890), big_data.get_time_index("a"));
        assert_eq!(vec!["a"], big_data.get_row_ids(1234567890));
        assert_eq!(None, insert_result);
        // second insert
        let insert_result = big_data.insert(RowData::new("a", r.clone(), 1234567891));
        assert_eq!(Some(1234567891), big_data.get_time_index("a"));
        assert_eq!(vec!["a"], big_data.get_row_ids(1234567891));
        assert_eq!(Some(RowData::new("a", r, 1234567890)), insert_result);
        //
        assert_eq!(Some(1234567891), big_data.get_time_index("a"));
        let aa: Vec<&str> = Vec::new();
        assert_eq!(aa, big_data.get_row_ids(1234567890));
        assert_eq!(vec!["a"], big_data.get_row_ids(1234567891))
    }
    #[test]
    fn get() {
        let mut big_data = BigData::new();
        let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
        let row1 = RowData::new("a", r.clone(), 1234567890);
        assert_eq!(None, big_data.get("a"));
        // none time_index
        assert_eq!(None, big_data.get_time_index("a"));
        let row_ids: Vec<&String> = Vec::new();
        // none row_ids
        assert_eq!(row_ids, big_data.get_row_ids(1234567890));
        big_data.insert(row1.clone());
        let row = big_data.get("a");
        assert_eq!(Some(1234567890), big_data.get_time_index("a"));
        let row_ids = vec!["a"];
        assert_eq!(row_ids, big_data.get_row_ids(1234567890));
        assert_eq!(row, Some(&row1))
    }
    fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128) {
        let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
        let row1 = RowData::new(row_id, r.clone(), *time);
        big_data.insert(row1);
    }
    #[test]
    fn get_time_index() {
        let mut big_data = BigData::new();
        assert_eq!(None, big_data.get_time_index("a"));
        insert_data(&mut big_data, "a", &1);
        insert_data(&mut big_data, "b", &2);
        assert_eq!(Some(1), big_data.get_time_index("a"));
        assert_eq!(Some(2), big_data.get_time_index("b"))
    }
    #[test]
    fn get_row_ids() {
        let mut big_data = BigData::new();
        let ids: Vec<&String> = Vec::new();
        assert_eq!(ids, big_data.get_row_ids(1));
        insert_data(&mut big_data, "a", &1);
        let mut ids = vec!["a"];
        assert_eq!(ids, big_data.get_row_ids(1));
        insert_data(&mut big_data, "b", &1);
        ids.push("b");
        assert_eq!(ids, big_data.get_row_ids(1));
    }
    #[test]
    fn get_range() {
        let mut big_data = BigData::new();
        // get range RowData
        let vec_data1: Vec<&RowData> = Vec::new();
        assert_eq!(vec_data1, big_data.get_range(0, 99999999999999));
        let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
        let row1 = RowData::new("a", r, 1234567890);
        big_data.insert(row1.clone());
        let vec_data = big_data.get_range(0, 1234567890);
        let vec_data1 = vec![&row1];
        assert_eq!(vec_data1, vec_data.clone());
        let vec_data = big_data.get_range(0, 1234567889);
        let vec_data1: Vec<&RowData> = Vec::new();
        assert_eq!(vec_data1, vec_data);
        // insert other RowData
        let r = RowTerm::Tuple(vec![RowTerm::List(vec![RowTerm::Atom("b".to_string())])]);
        let other = RowData::new("b", r, 1);
        big_data.insert(other.clone());
        let vec_data = big_data.get_range(0, 1234567890);
        let vec_data1 = vec![&other, &row1];
        assert_eq!(vec_data1, vec_data)
    }
    #[test]
    fn get_range_row_ids() {
        let mut big_data = BigData::new();
        let mut ids: Vec<&str> = Vec::new();
        assert_eq!(ids, big_data.get_range_row_ids(0, 1));
        insert_data(&mut big_data, "a", &0);
        ids.push("a");
        assert_eq!(ids, big_data.get_range_row_ids(0, 1));
        insert_data(&mut big_data, "b", &0);
        ids.push("b");
        assert_eq!(ids, big_data.get_range_row_ids(0, 1));
        insert_data(&mut big_data, "b", &0);
        insert_data(&mut big_data, "a", &1);
        ids.remove(0);
        ids.push("a");
        assert_eq!(ids, big_data.get_range_row_ids(0, 1));
        ids.remove(1);
        assert_eq!(ids, big_data.get_range_row_ids(0, 0));
        ids.remove(0);
        assert_eq!(ids, big_data.get_range_row_ids(2, 3));
    }
    #[test]
    fn remove() {
        let mut big_data = BigData::new();
        big_data.remove("0");
        insert_data(&mut big_data, "a", &1);
        assert_eq!(Some(1), big_data.get_time_index("a"));
        big_data.remove("a");
        assert_eq!(None, big_data.get_time_index("a"));
        assert_eq!(None, big_data.get("a"));
        let ids: Vec<&str> = Vec::new();
        assert_eq!(ids, big_data.get_row_ids(1));
    }
    #[test]
    fn remove_row_ids() {
        let mut big_data = BigData::new();
        big_data.remove_row_ids(0, 10);
        insert_data(&mut big_data, "0", &11);
        insert_data(&mut big_data, "1", &1);
        let mut ids: Vec<&str> = Vec::new();
        ids.push("1");
        ids.push("0");
        assert_eq!(ids, big_data.get_range_row_ids(0, 11));
        big_data.remove_row_ids(0, 10);
        ids.remove(0);
        assert_eq!(ids, big_data.get_range_row_ids(0, 11));
        big_data.remove_row_ids(11, 11);
        ids.remove(0);
        assert_eq!(ids, big_data.get_range_row_ids(0, 11));
    }
    #[test]
    fn clear() {
        let mut big_data = BigData::new();
        big_data.clear();
        insert_data(&mut big_data, "a", &1);
        big_data.clear();
        assert_eq!(None, big_data.get("a"));
        let ids: Vec<&str> = vec![];
        assert_eq!(ids, big_data.get_row_ids(1));
        assert_eq!(None, big_data.get_time_index("a"));
    }
    #[test]
    fn len_row_ids() {
        let mut big_data = BigData::new();
        insert_data(&mut big_data, "a", &0);
        insert_data(&mut big_data, "b", &1);
        assert_eq!(2, big_data.len());
        assert_eq!(1, big_data.len_row_ids(0));
        assert_eq!(1, big_data.len_row_ids(1));
        insert_data(&mut big_data, "b", &0);
        assert_eq!(2, big_data.len_row_ids(0));
        assert_eq!(0, big_data.len_row_ids(1));
    }
    #[test]
    fn len_range_row_ids() {
        let mut big_data = BigData::new();
        insert_data(&mut big_data, "a", &0);
        insert_data(&mut big_data, "b", &1);
        assert_eq!(2, big_data.len_range_row_ids(0, 1));
        insert_data(&mut big_data, "b", &0);
        assert_eq!(2, big_data.len_range_row_ids(0, 0));
        insert_data(&mut big_data, "c", &5);
        assert_eq!(2, big_data.len_range_row_ids(0, 4));
        assert_eq!(3, big_data.len_range_row_ids(0, 5));
    }
    #[test]
    fn update_elem() {
        let mut big_data = BigData::new();
        // single element
        let row = RowData::new("a", RowTerm::Integer(0), 1);
        big_data.insert(row);
        assert_eq!(
            vec![true],
            big_data.update_elem(
                "a",
                RowTerm::Tuple(vec![RowTerm::Integer(0), RowTerm::Integer(1)])
            )
        );
        assert_eq!(
            Some(&RowData::new("a", RowTerm::Integer(1), 1)),
            big_data.get("a")
        );
        // out scope
        assert_eq!(
            vec![false],
            big_data.update_elem(
                "a",
                RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Integer(2)])
            )
        );
        assert_eq!(
            Some(&RowData::new("a", RowTerm::Integer(1), 1)),
            big_data.get("a")
        );

        // update tuple
        big_data.clear();
        let row = RowData::new(
            "a",
            RowTerm::Tuple(vec![
                RowTerm::Integer(0),
                RowTerm::Atom("a".to_string()),
                RowTerm::Bitstring("b".to_string()),
            ]),
            1,
        );
        big_data.insert(row);
        big_data.update_elem(
            "a",
            RowTerm::Tuple(vec![RowTerm::Integer(0), RowTerm::Integer(1)]),
        );
        assert_eq!(
            Some(&RowData::new(
                "a",
                RowTerm::Tuple(vec![
                    RowTerm::Integer(1),
                    RowTerm::Atom("a".to_string()),
                    RowTerm::Bitstring("b".to_string())
                ]),
                1
            )),
            big_data.get("a")
        );

        big_data.update_elem(
            "a",
            RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("1".to_string())]),
        );

        assert_eq!(
            Some(&RowData::new(
                "a",
                RowTerm::Tuple(vec![
                    RowTerm::Integer(1),
                    RowTerm::Atom("1".to_string()),
                    RowTerm::Bitstring("b".to_string())
                ]),
                1
            )),
            big_data.get("a")
        );
        assert_eq!(
            vec![false],
            big_data.update_elem(
                "a",
                RowTerm::Tuple(vec![RowTerm::Integer(3), RowTerm::Atom("1".to_string())]),
            )
        );

        assert_eq!(
            Some(&RowData::new(
                "a",
                RowTerm::Tuple(vec![
                    RowTerm::Integer(1),
                    RowTerm::Atom("1".to_string()),
                    RowTerm::Bitstring("b".to_string())
                ]),
                1
            )),
            big_data.get("a")
        );

        // update list
        assert_eq!(
            vec![true, true, true],
            big_data.update_elem(
                "a",
                RowTerm::List(vec![
                    RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("2".to_string())]),
                    RowTerm::Tuple(vec![RowTerm::Integer(0), RowTerm::Integer(2)]),
                    RowTerm::Tuple(vec![
                        RowTerm::Integer(2),
                        RowTerm::Bitstring("c".to_string())
                    ])
                ])
            )
        );
        assert_eq!(
            Some(&RowData::new(
                "a",
                RowTerm::Tuple(vec![
                    RowTerm::Integer(2),
                    RowTerm::Atom("2".to_string()),
                    RowTerm::Bitstring("c".to_string())
                ]),
                1
            )),
            big_data.get("a")
        );
        assert_eq!(
            vec![true],
            big_data.update_elem(
                "a",
                RowTerm::List(vec![RowTerm::Tuple(vec![
                    RowTerm::Integer(0),
                    RowTerm::Tuple(vec![
                        RowTerm::Tuple(vec![
                            RowTerm::Atom("a".to_string()),
                            RowTerm::Atom("b".to_string())
                        ]),
                        RowTerm::List(vec![
                            RowTerm::Integer(1),
                            RowTerm::Integer(1),
                            RowTerm::Integer(1)
                        ]),
                        RowTerm::Integer(1),
                        RowTerm::Bitstring("hello".to_string())
                    ])
                ]),])
            )
        );

        assert_eq!(
            Some(&RowData::new(
                "a",
                RowTerm::Tuple(vec![
                    RowTerm::Tuple(vec![
                        RowTerm::Tuple(vec![
                            RowTerm::Atom("a".to_string()),
                            RowTerm::Atom("b".to_string())
                        ]),
                        RowTerm::List(vec![
                            RowTerm::Integer(1),
                            RowTerm::Integer(1),
                            RowTerm::Integer(1)
                        ]),
                        RowTerm::Integer(1),
                        RowTerm::Bitstring("hello".to_string())
                    ]),
                    RowTerm::Atom("2".to_string()),
                    RowTerm::Bitstring("c".to_string())
                ]),
                1
            )),
            big_data.get("a")
        );
    }
    #[test]
    fn update_counter() {
        let mut big_data = BigData::new();
        // single element
        let row = RowData::new("a", RowTerm::Integer(1), 1);
        big_data.insert(row);
        assert_eq!(
            vec![true],
            big_data.update_counter(
                "a",
                RowTerm::Tuple(vec![RowTerm::Integer(0), RowTerm::Integer(2)])
            )
        );
        assert_eq!(
            Some(&RowData::new("a", RowTerm::Integer(3), 1)),
            big_data.get("a")
        );
        // update tuple
        big_data.clear();
        let row = RowData::new(
            "a",
            RowTerm::Tuple(vec![
                RowTerm::Integer(1),
                RowTerm::Atom("a".to_string()),
                RowTerm::Bitstring("b".to_string()),
                RowTerm::Integer(2),
            ]),
            1,
        );
        big_data.insert(row);
        big_data.update_counter(
            "a",
            RowTerm::Tuple(vec![RowTerm::Integer(0), RowTerm::Integer(11)]),
        );
        assert_eq!(
            Some(&RowData::new(
                "a",
                RowTerm::Tuple(vec![
                    RowTerm::Integer(12),
                    RowTerm::Atom("a".to_string()),
                    RowTerm::Bitstring("b".to_string()),
                    RowTerm::Integer(2)
                ]),
                1
            )),
            big_data.get("a")
        );
    }
    #[test]
    fn to_list() {
        let mut big_data = BigData::new();
        let d = RowData::new("a", RowTerm::Integer(0), 10);
        big_data.insert(d.clone());
        let mut list = Vec::new();
        list.push(&d);
        assert_eq!(list, big_data.to_list());
        let e = RowData::new("e", RowTerm::Integer(0), 1);
        big_data.insert(e.clone());
        list.clear();
        list.push(&e);
        list.push(&d);
        assert_eq!(list, big_data.to_list());
    }
}
