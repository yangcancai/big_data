use core::big_data::RowData;
use core::big_data::RowTerm;
use core::term::binary_to_term;
use core::term::term_to_binary;
#[test]
fn term() {
    let bin = &[
        131, 104, 4, 100, 0, 8, 114, 111, 119, 95, 100, 97, 116, 97, 109, 0, 0, 0, 1, 49, 104, 2,
        100, 0, 1, 97, 100, 0, 1, 98, 110, 6, 0, 110, 46, 12, 225, 125, 1,
    ];
    let term: RowData = binary_to_term(bin).unwrap();
    assert_eq!(
        RowData::new(
            "1",
            RowTerm::Tuple(vec![RowTerm::Atom("a".into()), RowTerm::Atom("b".into())]),
            1640158211694
        ),
        term
    );
    let b: Vec<u8> = term_to_binary(term.clone()).unwrap();
    println!("src = {:?}, \ndecoded = {:?}", bin, b);
    let term1: RowData = binary_to_term(b.as_slice()).unwrap();
    assert_eq!(term, term1)
}
