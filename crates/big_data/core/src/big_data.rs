use ordermap::set::OrderSet;
use std::cmp::Ordering;

use anyhow::Error;
use std::collections::{BTreeMap, HashMap};
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Bound::Included;
pub type R<T> = anyhow::Result<T>;
#[cfg(not(feature = "nif"))]
use erlang_term::RawTerm;

pub struct Time(pub u128);

#[derive(Debug, Clone)]
pub struct RowOption {
    pub t: Option<String>,
    pub max_len: Option<i64>,
    pub update: Option<String>,
    // The replace cond
    pub replace_cond: Option<i64>,
}
impl From<RowTerm> for RowOption {
    fn from(r: RowTerm) -> Self {
        let mut rs = RowOption::new();
        if let RowTerm::List(list) = r {
            for option in list {
                if let RowTerm::Tuple(option) = option {
                    if let RowTerm::Atom(atom) = &option[0] {
                        match atom.as_str() {
                            "type" => {
                                if let RowTerm::Atom(t) = &option[1] {
                                    rs.set_t(t.clone());
                                }
                            }
                            "max_len" => {
                                if let RowTerm::Integer(max) = &option[1] {
                                    rs.set_max_len(*max);
                                }
                            }
                            "update" => {
                                if let RowTerm::Atom(update) = &option[1] {
                                    rs.set_update(update.clone());
                                }
                            }
                            "replace_cond" => {
                                if let RowTerm::Integer(replace_cond) = &option[1] {
                                    rs.set_replace_cond(*replace_cond);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        rs
    }
}
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
    // 0 -> default
    // Pos :: neg_integer() | string()
    // eg. current pos 5, and parent pos ..1
    // format: [
    // {Pos :: neg_integer(), [
    // {type, Type :: list | pos_integer()},
    // {max_len, Max :: pos_integer()}]},
    // {update, Compare :: gt | lt},
    // {replace_cond, pos_integer()}
    // ]
    options: HashMap<i32, RowTerm>,
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
#[cfg(feature = "nif")]
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
#[cfg(not(feature = "nif"))]
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
    Other(RawTerm),
}
impl Default for RowOption {
    fn default() -> Self {
        RowOption::new()
    }
}
impl RowOption {
    pub fn new() -> Self {
        Self {
            max_len: None,
            t: None,
            replace_cond: None,
            update: None,
        }
    }
    pub fn set_max_len(&mut self, max: i64) {
        self.max_len = Some(max);
    }
    pub fn set_t(&mut self, t: String) {
        self.t = Some(t);
    }
    pub fn set_update(&mut self, update: String) {
        self.update = Some(update);
    }
    pub fn set_replace_cond(&mut self, replace_cond: i64) {
        self.replace_cond = Some(replace_cond);
    }
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
            options: HashMap::new(),
        }
    }
    pub fn append_list(&mut self, row_data_list: Vec<RowData>, option: &RowTerm) {
        for row_data in row_data_list {
            let _ = self.append(row_data, option);
        }
    }
    /// Append RowData to the BigData
    ///
    ///  # Example
    ///
    ///
    ///
    pub fn append(&mut self, row_data: RowData, option: &RowTerm) -> R<()> {
        let time = row_data.time;
        let row_id = row_data.row_id.clone();
        match self.rows.get_mut(&row_id) {
            // first insert
            None => {
                self.rows.insert(row_id.clone(), row_data);
            }
            // second insert
            Some(r) => {
                let old_time = r.time;
                // clear old time_index
                if let Some(set) = self.time_index.get_mut(&old_time) {
                    set.remove(&row_id);
                }
                let rs = match &option {
                    // If option is equal integer, then fetch option from options
                    RowTerm::Integer(index) => {
                        let index = *index as i32;
                        match self.options.get(&index) {
                            Some(inner) => BigData::process_append(r, &row_data, inner),
                            _ => Ok(()),
                        }
                    }
                    _ => BigData::process_append(r, &row_data, option),
                };
                match rs {
                    Ok(()) => {}
                    Err(e) => return Err(e),
                }
            }
        }

        // insert time index
        if let Some(set) = self.time_index.get_mut(&time) {
            set.insert(row_id);
        } else {
            let mut set = OrderSet::new();
            set.insert(row_id);
            self.time_index.insert(time, Box::new(set));
        }
        Ok(())
    }

    ///  Process append, this is private function
    ///
    ///
    fn process_append(rs: &mut RowData, new: &RowData, option: &RowTerm) -> R<()> {
        // Supported options
        let mut filter = HashMap::new();
        // Default update all position
        // Only update location where it is set
        let mut only_update_location = false;
        match option {
            RowTerm::List(option) => {
                // [{pos,option}]
                for option in option {
                    match option {
                        RowTerm::Tuple(option) => {
                            // which element will be process with this option
                            let mut u = 0usize;
                            let pos = match &option[u] {
                                RowTerm::Integer(pos) => *pos as usize,
                                _ => return Err(Error::msg("Append option format error")),
                            };
                            u += 1;
                            let option: RowTerm = option[u].clone();
                            let option: RowOption = option.into();
                            // update
                            BigData::process_append_update(rs, new, pos, &option);
                            // replace_cond
                            // must before max_len
                            BigData::process_append_replace_cond(rs, new, pos, &option);
                            // max_len
                            BigData::process_append_max_len(rs, new, pos, &option);
                            filter.insert(pos, 1);
                        }
                        RowTerm::Atom(location) => {
                            if let "location" = location.as_str() {
                                only_update_location = true;
                            }
                        }
                        _ => return Err(Error::msg("Append option format error")),
                    }
                }
            }
            _ => return Err(Error::msg("Append option format error")),
        }
        if let (RowTerm::Tuple(inner), RowTerm::Tuple(other)) = (&mut rs.term, &new.term) {
            if !only_update_location || other.len() > inner.len() {
                for i in 0..other.len() {
                    // the pos option not included
                    if filter.get(&i).is_none() {
                        if inner.len() > i {
                            if !only_update_location {
                                inner[i] = other[i].clone();
                            }
                        } else {
                            // new elem append
                            inner.push(other[i].clone());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    ///
    ///  Process replace_cond
    ///
    #[allow(clippy::all)]
    fn process_append_replace_cond(
        rs: &mut RowData,
        new: &RowData,
        pos: usize,
        option: &RowOption,
    ) {
        if let Some(replace_cond) = option.replace_cond {
            let replace_cond = replace_cond as usize;
            if let Some(t) = &option.t {
                match (t.as_str(), &mut rs.term, &new.term) {
                    ("list", RowTerm::Tuple(tuple), RowTerm::Tuple(new_tuple)) => {
                        if tuple.len() > pos && new_tuple.len() > pos {
                            match (&mut tuple[pos], &new_tuple[pos]) {
                                (RowTerm::List(elem_list), RowTerm::Tuple(elem)) => {
                                    BigData::process_replace_elem(elem_list, replace_cond, elem);
                                }
                                (RowTerm::List(elem_list), RowTerm::List(elem)) => {
                                    for elem in elem {
                                        match elem {
                                            RowTerm::Tuple(elem) => {
                                                BigData::process_replace_elem(
                                                    elem_list,
                                                    replace_cond,
                                                    elem,
                                                );
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    #[allow(clippy::needless_range_loop, clippy::ptr_arg)]
    fn process_replace_elem(
        elem_list: &mut Vec<RowTerm>,
        replace_cond: usize,
        elem_tuple: &Vec<RowTerm>,
    ) {
        let mut replaced = false;
        for i in 0..elem_list.len() {
            let item = &mut elem_list[i];
            if let RowTerm::Tuple(item_tuple) = item {
                if item_tuple.len() > replace_cond
                    && elem_tuple.len() > replace_cond
                    && item_tuple[replace_cond] == elem_tuple[replace_cond]
                {
                    *item = RowTerm::Tuple((*elem_tuple).clone());
                    replaced = true;
                    break;
                }
            }
        }
        if !replaced {
            elem_list.push(RowTerm::Tuple((*elem_tuple).clone()));
        }
    }
    ///
    /// Process update option
    ///
    fn process_append_update(rs: &mut RowData, new: &RowData, pos: usize, option: &RowOption) {
        if let Some(update) = &option.update {
            match (&mut rs.term, &new.term, update.as_str()) {
                (RowTerm::Tuple(tuple), RowTerm::Tuple(new_tuple), "gt") => {
                    if new_tuple.len() > pos && tuple.len() > pos && new_tuple[pos] > tuple[pos] {
                        tuple[pos] = new_tuple[pos].clone();
                    }
                }
                (RowTerm::Tuple(tuple), RowTerm::Tuple(new_tuple), "lt") => {
                    if new_tuple.len() > pos && tuple.len() > pos && new_tuple[pos] < tuple[pos] {
                        tuple[pos] = new_tuple[pos].clone();
                    }
                }
                (RowTerm::Tuple(tuple), RowTerm::Tuple(new_tuple), "always") => {
                    if new_tuple.len() > pos && tuple.len() > pos {
                        tuple[pos] = new_tuple[pos].clone();
                    }
                }
                (a, b, "always") => {
                    *a = b.clone();
                }
                (a, b, "gt") => {
                    if b > a {
                        *a = b.clone();
                    }
                }
                (a, b, "lt") => {
                    if b < a {
                        *a = b.clone();
                    }
                }
                _ => {}
            }
        }
    }
    ///
    /// list element max_len
    ///
    fn process_append_max_len(rs: &mut RowData, new: &RowData, pos: usize, option: &RowOption) {
        if let Some(max) = option.max_len {
            if let Some(t) = &option.t {
                if let "list" = t.as_str() {
                    if let RowTerm::Tuple(list) = &mut rs.term {
                        if let RowTerm::Tuple(new_list) = &new.term {
                            if new_list.len() > pos && list.len() > pos {
                                if let RowTerm::List(list) = &mut list[pos] {
                                    if let RowTerm::List(new_list) = &new_list[pos] {
                                        // If replace_cond not None,
                                        // New elem will be replace with
                                        // process_append_replace_cond function
                                        if option.replace_cond.is_none() {
                                            let mut temp = (*new_list).clone();
                                            list.append(&mut temp);
                                        }
                                        if list.len() > max as usize {
                                            let new_pos = list.len() - max as usize;
                                            list.drain(..new_pos);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    ///
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
impl PartialOrd for RowTerm {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (RowTerm::Integer(inner), RowTerm::Integer(i)) => inner.partial_cmp(i),
            (RowTerm::Float(inner), RowTerm::Float(i)) => inner.partial_cmp(i),
            _ => None,
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
            #[cfg(not(feature = "nif"))]
            RowTerm::Other(self_inner) => match other {
                RowTerm::Other(inner) => self_inner == inner,
                _ => false,
            },
        }
    }
}
