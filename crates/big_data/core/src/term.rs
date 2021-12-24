use crate::big_data::RowData;
use crate::big_data::RowTerm;
use anyhow::Error;
use anyhow::Result;
use erlang_term::RawTerm;
use num_traits::ToPrimitive;

pub fn binary_to_term(binary: &[u8]) -> Result<Vec<RowData>> {
    let decoded = RawTerm::from_bytes(binary);
    match decoded {
        Ok(term) => raw_to_rowdata(term),
        Err(e) => Err(Error::msg(format!("RawTerm::from_bytes crash, {}", e))),
    }
}
pub fn binary_to_rowterm(binary: &[u8]) -> Result<RowTerm>{
    let decoded = RawTerm::from_bytes(binary);
    match decoded {
        Ok(term) => to_row_term(term),
        Err(e) => Err(Error::msg(format!("RawTerm::from_bytes crash, {}", e))),
    }
}
pub fn list_to_binary(list: &[RowData]) -> Result<Vec<u8>> {
    let mut rs = vec![];
    for row in list.iter() {
        rs.push(rowdata_to_raw(row)?);
    }
    Ok(RawTerm::List(rs).to_bytes())
}
pub fn parse_time(time: &RawTerm) -> Result<u128> {
    match time {
        RawTerm::SmallBigInt(time) | RawTerm::LargeBigInt(time) => Ok(time.to_u128().unwrap()),
        RawTerm::SmallInt(time) => Ok(time.to_u128().unwrap()),
        RawTerm::Int(time) => Ok(time.to_u128().unwrap()),
        _ => Err(Error::msg(format!("RowData.time invalid : {:?}", time))),
    }
}
fn is_row_data(term: &RawTerm) -> bool {
    match term {
        RawTerm::Atom(a)
        | RawTerm::AtomDeprecated(a)
        | RawTerm::SmallAtom(a)
        | RawTerm::SmallAtomDeprecated(a) => "row_data" == a,
        _ => false,
    }
}
pub fn raw_to_rowdata(term: RawTerm) -> Result<Vec<RowData>> {
    if let RawTerm::SmallTuple(tuple) = term {
        if tuple.len() == 4 {
            if is_row_data(&tuple[0]) {
                if let RawTerm::Binary(row_id) = &tuple[1] {
                    match &tuple[2] {
                        RawTerm::SmallTuple(tuple1) | RawTerm::LargeTuple(tuple1) => {
                            let time = parse_time(&tuple[3])?;
                            return Ok(vec![RowData::new(
                                std::str::from_utf8(row_id)?,
                                to_row_term(RawTerm::SmallTuple(tuple1.clone()))?,
                                time,
                            )]);
                        }
                        _ => {}
                    }
                }
            } else {
                return Err(Error::msg(
                    "RowData tuple invalid: The record must be row_data",
                ));
            }
        } else {
            return Err(Error::msg("RowData tuple invalid: The tuple len must be 4"));
        }
    } else if let RawTerm::List(list) = term {
        let mut rs = vec![];
        for row in list {
            rs.extend(raw_to_rowdata(row)?);
        }
        return Ok(rs);
    }
    Err(Error::msg("RowData tuple invalid"))
}
pub fn rowdata_to_raw(row: &RowData) -> Result<RawTerm> {
    let row_id = RawTerm::Binary(row.row_id.as_bytes().to_vec());
    let term = to_raw_term(row.term.clone())?;
    let time = RawTerm::SmallBigInt(row.time.into());
    Ok(RawTerm::SmallTuple(vec![
        to_raw_term(RowTerm::Atom("row_data".into()))?,
        row_id,
        term,
        time,
    ]))
}
pub fn list_atom_to_raw(list: Vec<bool>) -> Result<RawTerm>{
    let mut rs = vec![];
    for row in list{
        rs.push(RawTerm::Atom(row.to_string()));
    }
    Ok(RawTerm::List(rs))
}
pub fn term_to_binary(row: &RowData) -> Result<Vec<u8>> {
    Ok(rowdata_to_raw(row)?.to_bytes())
}
fn to_row_term(raw: RawTerm) -> Result<RowTerm> {
    match raw {
        RawTerm::Atom(a)
        | RawTerm::AtomDeprecated(a)
        | RawTerm::SmallAtom(a)
        | RawTerm::SmallAtomDeprecated(a) => Ok(RowTerm::Atom(a)),
         RawTerm::SmallBigInt(i) | RawTerm::LargeBigInt(i) => {
             Ok(RowTerm::Integer(i.to_i64().unwrap()))
         }
        RawTerm::SmallInt(i) => Ok(RowTerm::Integer(i.to_i64().unwrap())),
        RawTerm::Int(i) => Ok(RowTerm::Integer(i.to_i64().unwrap())),
        RawTerm::SmallTuple(tuple) | RawTerm::LargeTuple(tuple) => {
            let mut t: Vec<RowTerm> = Vec::new();
            for row in tuple.iter() {
                t.push(to_row_term(row.clone())?);
            }
            Ok(RowTerm::Tuple(t))
        }
        RawTerm::List(list) => {
            let mut t: Vec<RowTerm> = Vec::new();
            for row in list.iter() {
                t.push(to_row_term(row.clone())?);
            }
            Ok(RowTerm::List(t))
        }
        RawTerm::String(str) => Ok(RowTerm::Bitstring(String::from_utf8(str)?)),
        RawTerm::Binary(bin) => Ok(RowTerm::Bin(bin)),
        _ => Err(Error::msg("RowTerm tuple invalid")),
    }
}
fn to_raw_term(row: RowTerm) -> Result<RawTerm> {
    match row {
        RowTerm::Integer(i) => Ok(big_int_to_raw_term(i)),
        RowTerm::Atom(atom) => Ok(atom_to_raw_term(atom)),
        RowTerm::Bitstring(str) => Ok(RawTerm::String(str.as_bytes().to_vec())),
        RowTerm::Bin(bin) => Ok(RawTerm::Binary(bin)),
        RowTerm::Tuple(tuple) => tuple_to_raw_term(tuple),
        RowTerm::List(list) => list_to_raw_term(list),
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
        Ok(RawTerm::List(t))
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
    if (0..=255).contains(&input) {
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
