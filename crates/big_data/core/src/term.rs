use crate::big_data::BigData;
use crate::big_data::RowData;
use crate::big_data::RowTerm;
use crate::traits::FromBytes;
use crate::traits::ToBytes;
use anyhow::Error;
use anyhow::Result;
use erlang_term::RawTerm;
use num_traits::ToPrimitive;
pub enum ErlRes<T, E> {
    Ok,
    OkTuple(T),
    NotFound,
    Err(E),
    ErrString(String),
}
impl ToBytes for &RowTerm {
    fn to_bytes(self) -> Result<Vec<u8>> {
        let raw = to_raw_term((*self).clone())?;
        Ok(raw.to_bytes())
    }
}
impl ToBytes for RowTerm {
    fn to_bytes(self) -> Result<Vec<u8>> {
        Ok(to_raw_term(self)?.to_bytes())
    }
}
impl ToBytes for Vec<&RowTerm> {
    fn to_bytes(self) -> Result<Vec<u8>> {
        let mut rs = vec![];
        for row in self {
            let raw = (*row).clone();
            rs.push(raw);
        }
        Ok(to_raw_term(RowTerm::Tuple(rs))?.to_bytes())
    }
}
impl ToBytes for &[RowData] {
    fn to_bytes(self) -> Result<Vec<u8>> {
        let mut rs = vec![];
        for row in self.iter() {
            rs.push(rowdata_to_raw(row)?);
        }
        Ok(RawTerm::List(rs).to_bytes())
    }
}
impl ToBytes for ErlRes<RowTerm, RowTerm> {
    fn to_bytes(self) -> Result<Vec<u8>> {
        match self {
            ErlRes::Ok => Ok(atom_to_raw_term("ok".into()).to_bytes()),
            ErlRes::OkTuple(value) => Ok(ok(value)?.to_bytes()),
            ErlRes::NotFound => Ok(atom_to_raw_term("notfound".into()).to_bytes()),
            ErlRes::Err(e) => {
                Ok(to_raw_term(RowTerm::Tuple(vec![RowTerm::Atom("error".into()), e]))?.to_bytes())
            }
            ErlRes::ErrString(e) => Ok(to_raw_term(RowTerm::Tuple(vec![
                RowTerm::Atom("error".into()),
                RowTerm::Atom(e),
            ]))?
            .to_bytes()),
        }
    }
}
impl ToBytes for Vec<&RowData> {
    fn to_bytes(self) -> Result<Vec<u8>> {
        let mut rs = vec![];
        for row in self {
            rs.push(rowdata_to_raw(row)?)
        }
        Ok(RawTerm::List(rs).to_bytes())
    }
}
impl ToBytes for BigData {
    fn to_bytes(self) -> Result<Vec<u8>> {
        self.to_list().to_bytes()
    }
}
impl ToBytes for &BigData {
    fn to_bytes(self) -> Result<Vec<u8>> {
        self.to_list().to_bytes()
    }
}
impl ToBytes for &RowData {
    fn to_bytes(self) -> Result<Vec<u8>> {
        Ok(rowdata_to_raw(self)?.to_bytes())
    }
}
impl ToBytes for &[RawTerm] {
    fn to_bytes(self) -> Result<Vec<u8>> {
        Ok(RawTerm::List(self.to_vec()).to_bytes())
    }
}
impl ToBytes for Vec<RawTerm> {
    fn to_bytes(self) -> Result<Vec<u8>> {
        Ok(RawTerm::List(self.to_vec()).to_bytes())
    }
}
impl ToBytes for Vec<&RawTerm> {
    fn to_bytes(self) -> Result<Vec<u8>> {
        let mut rs = vec![];
        for row in self {
            rs.push((*row).clone());
        }
        Ok(RawTerm::List(rs).to_bytes())
    }
}
impl ToBytes for Vec<bool> {
    fn to_bytes(self) -> Result<Vec<u8>> {
        match list_atom_to_raw(self) {
            Ok(raw_term) => Ok(raw_term.to_bytes()),
            Err(e) => Err(Error::msg(format!("Vec<bool>: to_bytes crash: {}", e))),
        }
    }
}
impl ToBytes for Vec<&String> {
    fn to_bytes(self) -> Result<Vec<u8>> {
        let mut rs = vec![];
        for row in self {
            rs.push(to_raw_term(RowTerm::Bin(
                (*row).clone().as_bytes().to_vec(),
            ))?);
        }
        Ok(RawTerm::List(rs).to_bytes())
    }
}
impl ToBytes for Option<u128> {
    fn to_bytes(self) -> Result<Vec<u8>> {
        match self {
            Some(value) => Ok(to_raw_term(RowTerm::Integer(value as i64))?.to_bytes()),
            None => ErlRes::NotFound.to_bytes(),
        }
    }
}
impl FromBytes for BigData {
    fn from_bytes(b: &[u8]) -> Result<Self> {
        let r: Result<Vec<RowData>> = Vec::<RowData>::from_bytes(b);
        let mut rs = BigData::new();
        match r {
            Ok(term) => {
                rs.insert_list(term);
                Ok(rs)
            }
            Err(_) => Ok(rs),
        }
    }
}
impl FromBytes for Vec<RowData> {
    fn from_bytes(binary: &[u8]) -> Result<Self> {
        let decoded = RawTerm::from_bytes(binary);
        match decoded {
            Ok(term) => raw_to_rowdata(term),
            Err(e) => Err(Error::msg(format!("RawTerm::from_bytes crash, {}", e))),
        }
    }
}
impl FromBytes for RowTerm {
    fn from_bytes(binary: &[u8]) -> Result<RowTerm> {
        let decoded = RawTerm::from_bytes(binary);
        match decoded {
            Ok(term) => to_row_term(term),
            Err(e) => Err(Error::msg(format!("RawTerm::from_bytes crash, {}", e))),
        }
    }
}
pub fn ok(row: RowTerm) -> Result<RawTerm> {
    to_raw_term(RowTerm::Tuple(vec![RowTerm::Atom("ok".into()), row]))
}
pub fn error(atom: String) -> Result<RawTerm> {
    to_raw_term(RowTerm::Tuple(vec![
        RowTerm::Atom("error".into()),
        RowTerm::Atom(atom),
    ]))
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
    if let RawTerm::SmallTuple(tuple) | RawTerm::LargeTuple(tuple) = term {
        if tuple.len() == 4 {
            if is_row_data(&tuple[0]) {
                if let RawTerm::Binary(row_id) = &tuple[1] {
                    let time = parse_time(&tuple[3])?;
                    return Ok(vec![RowData::new(
                        std::str::from_utf8(row_id)?,
                        to_row_term(tuple[2].clone())?,
                        time,
                    )]);
                } else {
                    return Err(Error::msg(
                        "RowData tuple invalid: The row_id is not binary",
                    ));
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
    Err(Error::msg(format!("RowData tuple invalid {:?}", term)))
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
pub fn list_atom_to_raw(list: Vec<bool>) -> Result<RawTerm> {
    let mut rs = vec![];
    for row in list {
        rs.push(RawTerm::Atom(row.to_string()));
    }
    Ok(RawTerm::List(rs))
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
pub fn to_raw_term(row: RowTerm) -> Result<RawTerm> {
    match row {
        RowTerm::Integer(i) => Ok(big_int_to_raw_term(i)),
        RowTerm::Atom(atom) => Ok(atom_to_raw_term(atom)),
        RowTerm::Bitstring(str) => Ok(RawTerm::String(str.as_bytes().to_vec())),
        RowTerm::Bin(bin) => Ok(RawTerm::Binary(bin)),
        RowTerm::Tuple(tuple) => tuple_to_raw_term(tuple),
        RowTerm::List(list) => list_to_raw_term(list),
    }
}

pub fn list_to_raw_term(list: Vec<RowTerm>) -> Result<RawTerm> {
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
