//-------------------------------------------------------------------
// @author yangcancai

// Copyright (c) 2021 by yangcancai(yangcancai0112@gmail.com), All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
   
// @doc
//
// @end
// Created : 2022-01-24T03:22:39+00:00
//-------------------------------------------------------------------
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
use std::io::Write;
use std::convert::TryFrom;
use crate::big_data::RowTerm;
use crate::big_data::RowData;
use crate::big_data::Time;
use super::atoms;
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
            // RowTerm::Other(term) => {
            //        *term
            //     }
        }
    }
}
impl <'a>Decoder<'a> for RowTerm {
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
impl <'a>Decoder<'a> for RowData{
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
        // Some(RowTerm::Other(&term))
    }
}
