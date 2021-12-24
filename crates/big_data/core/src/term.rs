use crate::big_data::RowData;
use crate::big_data::RowTerm;
use anyhow::Error;
use anyhow::Result;
use erlang_term::RawTerm;
use num_traits::ToPrimitive;

pub fn binary_to_term(binary: &[u8]) -> Result<RowData> {
    let decoded = RawTerm::from_bytes(binary);
    if decoded.is_ok() {
        let term: RawTerm = decoded.unwrap();
        if let RawTerm::SmallTuple(tuple) = term {
            // tuple[0] == row_data
            if let RawTerm::Binary(row_id) = &tuple[1] {
                if let RawTerm::SmallTuple(tuple1) = &tuple[2] {
                    if let RawTerm::SmallBigInt(time) = &tuple[3] {
                        return Ok(RowData::new(
                            std::str::from_utf8(&row_id)?,
                            to_row_term(RawTerm::SmallTuple(tuple1.clone()))?,
                            time.to_u128().unwrap(),
                        ));
                    }
                }
            }
        }
    } else if let Err(e) = decoded {
        return Err(Error::msg(format!("RawTerm::from_bytes crash, {}", e)));
    }
    Err(Error::msg("RowData tuple invalid"))
}
pub fn list_to_binary(list: &[RowData]) -> Result<Vec<u8>>{
    let mut rs = vec![];
    for row in list.iter(){
        rs.push(rowdata_to_raw(row)?);
    }
    Ok(RawTerm::List(rs).to_bytes())
}
pub fn rowdata_to_raw(row: &RowData) -> Result<RawTerm> {
    let row_id = RawTerm::Binary(row.row_id.as_bytes().to_vec());
    let term = to_raw_term(row.term.clone())?;
    let time = RawTerm::SmallBigInt(row.time.into());
    Ok(RawTerm::SmallTuple(vec![to_raw_term(RowTerm::Atom("row_data".into()))?, row_id, term, time]))
}
pub fn term_to_binary(row: &RowData) -> Result<Vec<u8>> {
    Ok(rowdata_to_raw(row)?.to_bytes())
}
fn to_row_term(raw: RawTerm) -> Result<RowTerm> {
    match raw {
        RawTerm::Atom(a) | RawTerm::AtomDeprecated(a) | 
        RawTerm::SmallAtom(a) | RawTerm::SmallAtomDeprecated(a) => {
            return Ok(RowTerm::Atom(a));
        }
        // RawTerm::SmallBigInt(i) | RawTerm::LargeBigInt(i) => {
        //     return Ok(RowTerm::Integer(i.to_i64().unwrap()));
        // }
        RawTerm::SmallInt(i) =>{
            return Ok(RowTerm::Integer(i.to_i64().unwrap()));
        }
        RawTerm::Int(i) =>
        {
            return Ok(RowTerm::Integer(i.to_i64().unwrap()));
        }
        RawTerm::SmallTuple(tuple) | RawTerm::LargeTuple(tuple) => {
            let mut t: Vec<RowTerm> = Vec::new();
            for row in tuple.iter() {
                t.push(to_row_term(row.clone())?);
            }
            return Ok(RowTerm::Tuple(t));
        }
        RawTerm::List(list) => {
            let mut t: Vec<RowTerm> = Vec::new();
            for row in list.iter() {
                t.push(to_row_term(row.clone())?);
            }
            return Ok(RowTerm::List(t));
        }
        RawTerm::String(str) => {
            return Ok(RowTerm::Bitstring(String::from_utf8(str)?));
        }
        RawTerm::Binary(bin) => {
            return Ok(RowTerm::Bin(bin));
        }
        _ => {
            return Err(Error::msg("RowTerm tuple invalid"));
        }
    }
}
fn to_raw_term(row: RowTerm) -> Result<RawTerm> {
    match row {
        RowTerm::Integer(i) => {
            return Ok(big_int_to_raw_term(i));
        }
        RowTerm::Atom(atom) => {
           return Ok(atom_to_raw_term(atom));
        }
        RowTerm::Bitstring(str) => {
            return Ok(RawTerm::String(str.as_bytes().to_vec()));
        }
        RowTerm::Bin(bin) => {
            return Ok(RawTerm::Binary(bin));
        }
        RowTerm::Tuple(tuple) => {
            return tuple_to_raw_term(tuple);
                   },
        RowTerm::List(list) => {
            return list_to_raw_term(list);
        }
    }
}

fn list_to_raw_term(list: Vec<RowTerm>) -> Result<RawTerm> {
    if list.is_empty() {
        Ok(RawTerm::Nil)
    } else {
         let mut t: Vec<RawTerm> = Vec::new();
            for row in list.iter() {
                t.push(to_raw_term(row.clone())?);
            }
            return Ok(RawTerm::List(t));
    }
}

fn tuple_to_raw_term(tuple: Vec<RowTerm>) -> Result<RawTerm> {
    let len = tuple.len();
     let mut x: Vec<RawTerm> = Vec::new();
    for row in tuple.iter() {
    x.push(to_raw_term(row.clone())?);
    }
    if len < 16 {
        Ok(RawTerm::SmallTuple(x))
    } else {
        Ok(RawTerm::LargeTuple(x))
    }
}

fn big_int_to_raw_term(input: i64) -> RawTerm {
    if input <= 255 && input >= 0{
        RawTerm::SmallBigInt(input.into())
    } else {
        RawTerm::LargeBigInt(input.into())
    }
}

fn atom_to_raw_term(input: String) -> RawTerm {
    if input.len() < 256 {
        RawTerm::SmallAtom(input)
    } else {
        RawTerm::Atom(input)
    }
}