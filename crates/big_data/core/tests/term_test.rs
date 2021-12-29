use erlang_term::RawTerm;
use core::big_data::RowData;
use core::big_data::RowTerm;
use core::traits::FromBytes;
use core::traits::ToBytes;
#[test]
fn term() {
    let bin = &[
        131, 104, 4, 100, 0, 8, 114, 111, 119, 95, 100, 97, 116, 97, 109, 0, 0, 0, 1, 49, 104, 2,
        100, 0, 1, 97, 100, 0, 1, 98, 110, 6, 0, 110, 46, 12, 225, 125, 1,
    ];
    let term = Vec::<RowData>::from_bytes(bin).unwrap();
    assert_eq!(
        RowData::new(
            "1",
            RowTerm::Tuple(vec![RowTerm::Atom("a".into()), RowTerm::Atom("b".into())]),
            1640158211694
        ),
        term[0]
    );
    let b: Vec<u8> = term.to_bytes().unwrap();
    let term1 = Vec::<RowData>::from_bytes(b.as_slice()).unwrap();
    assert_eq!(term, term1);
    let b = &[131,107,0,1,0];
    let r = RawTerm::from_bytes(b);
    assert_eq!(RawTerm::String(vec![0]), r.unwrap());
    let r = RowTerm::from_bytes(b);
    let v: Vec<u8> = vec![0];
    assert_eq!(RowTerm::Bitstring(String::from_utf8(v).unwrap()), r.unwrap());
}
