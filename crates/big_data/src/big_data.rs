use std::collections::{BTreeMap, HashMap, HashSet};

use atoms;
use rustler::types::atom::Atom;
use rustler::types::tuple::make_tuple;
use rustler::Encoder;
use rustler::Env;
use rustler::Term;
use std::ops::Bound::Included;
use ordermap::set::OrderSet;

#[derive(Debug, Clone)]
pub struct RowData {
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
    pub fn new(row_term: RowTerm, time: u128) -> Self {
        RowData {
            term: row_term,
            time: time,
        }
    }
}
impl PartialEq for RowData {
    fn eq(&self, other: &RowData) -> bool {
        if self.term == other.term {
            return self.time == self.time;
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
    ///    let mut big_data = BigData::new();
    ///    let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
    ///    let insert_result = big_data.insert("a", RowData::new(r.clone(), 1234567890));
    ///    assert_eq!(None, insert_result);
    /// ```
    ///
    pub fn insert(&mut self, row_id: &str, row_data: RowData) -> Option<RowData> {
        let time = row_data.time;
        let rs = match self.rows.insert(row_id.to_string(), row_data) {
            // first insert
            None => None,
            // second insert
            Some(r) => {
                let old_time = r.time;
                // clear old time_index
                if let Some(set) = self.time_index.get_mut(&old_time) {
                    set.remove(row_id);
                }
                Some(r)
            }
        };
        if let Some(set) = self.time_index.get_mut(&time) {
            set.insert(row_id.to_string());
        } else {
            let mut set = OrderSet::new();
            set.insert(row_id.to_string());
            self.time_index.insert(time, Box::new(set));
        }
        rs
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
        match self.get(row_id) {
            None => None,
            Some(row) => Some(row.time),
        }
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
                let terms: Vec<_> = inner.into_iter().map(|t| t.encode(env)).collect();
                make_tuple(env, terms.as_ref()).encode(env)
            }
            RowTerm::List(inner) => inner.encode(env),
            RowTerm::Bitstring(inner) => inner.encode(env),
        }
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
        let insert_result = big_data.insert("a", RowData::new(r.clone(), 1234567890));
        assert_eq!(Some(1234567890), big_data.get_time_index("a"));
        assert_eq!(vec!["a"], big_data.get_row_ids(1234567890));
        assert_eq!(None, insert_result);
        // second insert
        let insert_result = big_data.insert("a", RowData::new(r.clone(), 1234567891));
        assert_eq!(Some(1234567891), big_data.get_time_index("a"));
        assert_eq!(vec!["a"], big_data.get_row_ids(1234567891));
        assert_eq!(Some(RowData::new(r, 1234567890)), insert_result);
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
        let row1 = RowData::new(r.clone(), 1234567890);
        assert_eq!(None, big_data.get("a"));
        // none time_index
        assert_eq!(None, big_data.get_time_index("a"));
        let row_ids: Vec<&String> = Vec::new();
        // none row_ids
        assert_eq!(row_ids, big_data.get_row_ids(1234567890));
        big_data.insert("a", row1.clone());
        let row = big_data.get("a");
        assert_eq!(Some(1234567890), big_data.get_time_index("a"));
        let row_ids = vec!["a"];
        assert_eq!(row_ids, big_data.get_row_ids(1234567890));
        assert_eq!(row, Some(&row1))
    }
    fn insert_data(big_data: &mut BigData, row_id: &str, time: &u128) {
        let r = RowTerm::Tuple(vec![RowTerm::Integer(1), RowTerm::Atom("a".to_string())]);
        let row1 = RowData::new(r.clone(), *time);
        big_data.insert(row_id, row1);
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
        let row1 = RowData::new(r, 1234567890);
        big_data.insert("a", row1.clone());
        let vec_data = big_data.get_range(0, 1234567890);
        let vec_data1 = vec![&row1];
        assert_eq!(vec_data1, vec_data.clone());
        let vec_data = big_data.get_range(0, 1234567889);
        let vec_data1: Vec<&RowData> = Vec::new();
        assert_eq!(vec_data1, vec_data);
        // insert other RowData
        let r = RowTerm::Tuple(vec![RowTerm::List(vec![RowTerm::Atom("b".to_string())])]);
        let other = RowData::new(r, 1);
        big_data.insert("b", other.clone());
        let vec_data = big_data.get_range(0, 1234567890);
        let vec_data1 = vec![&other, &row1];
        assert_eq!(vec_data1, vec_data)
    }
    #[test]
    fn get_range_row_ids() {
        let mut big_data = BigData::new();
        let mut ids: Vec<&str> = Vec::new();
        assert_eq!(ids, big_data.get_range_row_ids(0,1));
        insert_data(&mut big_data, "a", &0);
        ids.push("a");
        assert_eq!(ids, big_data.get_range_row_ids(0,1));
        insert_data(&mut big_data, "b", &0);
        ids.push("b");
        assert_eq!(ids, big_data.get_range_row_ids(0,1));
        insert_data(&mut big_data, "b", &0);
        insert_data(&mut big_data, "a", &1);
        ids.remove(0);
        ids.push("a");
        assert_eq!(ids, big_data.get_range_row_ids(0,1));
        ids.remove(1);
        assert_eq!(ids, big_data.get_range_row_ids(0,0));
        ids.remove(0);
        assert_eq!(ids, big_data.get_range_row_ids(2,3));
    }
}
