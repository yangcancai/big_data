extern crate core;
use core::big_data::BigData;
use core::big_data::RowData;
use core::big_data::RowOption;
use core::big_data::RowTerm;
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
#[test]
fn rowterm_eq() {
    let a = RowTerm::Bin(vec![1, 2, 3]);
    let b = RowTerm::Bin(vec![1, 2, 3]);
    assert_eq!(a, b)
}
#[test]
fn lookup_elem() {
    let mut big_data = BigData::new();
    let d = RowData::new("1", RowTerm::Integer(1), 10);
    big_data.insert(d.clone());
    let rs = big_data.lookup_elem("1", RowTerm::List(vec![RowTerm::Integer(0)]));
    assert_eq!(rs, vec![&RowTerm::Integer(1)]);
}

#[test]
fn rowterm2rowoption() {
    let option = RowTerm::List(vec![RowTerm::Tuple(vec![
        RowTerm::Atom("update".into()),
        RowTerm::Atom("lt".into()),
    ])]);
    let row_option: RowOption = option.into();
    assert_eq!(row_option.update, Some("lt".into()));
    let option = RowTerm::List(vec![
        RowTerm::Tuple(vec![
            RowTerm::Atom("update".into()),
            RowTerm::Atom("lt".into()),
        ]),
        RowTerm::Tuple(vec![
            RowTerm::Atom("type".into()),
            RowTerm::Atom("list".into()),
        ]),
        RowTerm::Tuple(vec![RowTerm::Atom("max_len".into()), RowTerm::Integer(10)]),
        RowTerm::Tuple(vec![
            RowTerm::Atom("replace_cond".into()),
            RowTerm::Integer(1),
        ]),
    ]);
    let row_option: RowOption = option.into();
    assert_eq!(row_option.update, Some("lt".into()));
    assert_eq!(row_option.max_len, Some(10));
    assert_eq!(row_option.t, Some("list".into()));
    assert_eq!(row_option.replace_cond, Some(1));
}
#[test]
fn append() {
    // term is equal integer
    let mut big_data = BigData::new();
    let d = RowData::new("1", RowTerm::Integer(1), 10);
    let option = RowTerm::List(vec![RowTerm::Tuple(vec![
        RowTerm::Integer(0),
        RowTerm::List(vec![RowTerm::Tuple(vec![
            RowTerm::Atom("update".into()),
            RowTerm::Atom("lt".into()),
        ])]),
    ])]);
    let _ = big_data.append(d.clone(), &option);
    let rs = big_data.lookup_elem("1", RowTerm::List(vec![RowTerm::Integer(0)]));
    assert_eq!(rs, vec![&RowTerm::Integer(1)]);
    let d = RowData::new("1", RowTerm::Integer(0), 10);
    let _ = big_data.append(d.clone(), &option);
    let rs = big_data.lookup_elem("1", RowTerm::List(vec![RowTerm::Integer(0)]));
    assert_eq!(rs, vec![&RowTerm::Integer(0)]);
    // term is equal float
    let mut big_data = BigData::new();
    let d = RowData::new("1", RowTerm::Float(1.0), 10);
    let option = RowTerm::List(vec![RowTerm::Tuple(vec![
        RowTerm::Integer(0),
        RowTerm::List(vec![RowTerm::Tuple(vec![
            RowTerm::Atom("update".into()),
            RowTerm::Atom("lt".into()),
        ])]),
    ])]);
    let _ = big_data.append(d.clone(), &option);
    let rs = big_data.lookup_elem("1", RowTerm::List(vec![RowTerm::Integer(0)]));
    assert_eq!(rs, vec![&RowTerm::Float(1.0)]);
    let d = RowData::new("1", RowTerm::Float(0.0), 10);
    let _ = big_data.append(d.clone(), &option);
    let rs = big_data.lookup_elem("1", RowTerm::List(vec![RowTerm::Integer(0)]));
    assert_eq!(rs, vec![&RowTerm::Float(0.0)]);
    // term is equal tuple
    let mut big_data = BigData::new();
    // term = {0, [{a, 3}]}
    let d = RowData::new(
        "1",
        RowTerm::Tuple(vec![
            RowTerm::Integer(0),
            RowTerm::List(vec![RowTerm::Tuple(vec![
                RowTerm::Atom("a".into()),
                RowTerm::Integer(3),
            ])]),
        ]),
        10,
    );
    let option = RowTerm::List(vec![
        // pos = 0
        RowTerm::Tuple(vec![
            RowTerm::Integer(0),
            RowTerm::List(vec![RowTerm::Tuple(vec![
                RowTerm::Atom("update".into()),
                RowTerm::Atom("gt".into()),
            ])]),
        ]),
        // pos = 1
        RowTerm::Tuple(vec![
            RowTerm::Integer(1),
            RowTerm::List(vec![
                RowTerm::Tuple(vec![RowTerm::Atom("max_len".into()), RowTerm::Integer(1)]),
                RowTerm::Tuple(vec![
                    RowTerm::Atom("type".into()),
                    RowTerm::Atom("list".into()),
                ]),
            ]),
        ]),
    ]);
    let _ = big_data.append(d.clone(), &option);
    let rs = big_data.lookup_elem("1", RowTerm::List(vec![RowTerm::Integer(0)]));
    assert_eq!(rs, vec![&RowTerm::Integer(0)]);
    let d = RowData::new(
        "1",
        RowTerm::Tuple(vec![
            RowTerm::Integer(0),
            RowTerm::List(vec![
                RowTerm::Tuple(vec![RowTerm::Atom("b".into()), RowTerm::Integer(4)]),
                RowTerm::Tuple(vec![RowTerm::Atom("c".into()), RowTerm::Integer(5)]),
            ]),
        ]),
        10,
    );
    let _ = big_data.append(d.clone(), &option);
    let rs = big_data.lookup_elem("1", RowTerm::Integer(1));
    assert_eq!(
        rs,
        vec![&RowTerm::List(vec![RowTerm::Tuple(vec![
            RowTerm::Atom("c".into()),
            RowTerm::Integer(5),
        ]),])]
    );
}

#[test]
fn append_replace_cond() {
    let mut big_data = BigData::new();
    let option = RowTerm::List(vec![
        // pos = 0
        RowTerm::Tuple(vec![
            RowTerm::Integer(0),
            RowTerm::List(vec![RowTerm::Tuple(vec![
                RowTerm::Atom("update".into()),
                RowTerm::Atom("gt".into()),
            ])]),
        ]),
        // pos = 1
        RowTerm::Tuple(vec![
            RowTerm::Integer(1),
            RowTerm::List(vec![
                RowTerm::Tuple(vec![
                    RowTerm::Atom("replace_cond".into()),
                    RowTerm::Integer(1),
                ]),
                RowTerm::Tuple(vec![RowTerm::Atom("max_len".into()), RowTerm::Integer(10)]),
                RowTerm::Tuple(vec![
                    RowTerm::Atom("type".into()),
                    RowTerm::Atom("list".into()),
                ]),
            ]),
        ]),
    ]);
    let _ = big_data.append(get_rowdata(|| get_term("a")), &option);
    let _ = big_data.append(get_rowdata(|| get_term("b")), &option);
    let _ = big_data.append(get_rowdata(|| get_term_key("c", 4)), &option);
    let _ = big_data.append(
        get_rowdata(|| add_new_elem(|| get_term_key("e", 5), || RowTerm::Integer(90))),
        &option,
    );
    let rs = big_data.lookup_elem("1", RowTerm::Integer(1));
    assert_eq!(
        rs,
        vec![&RowTerm::List(vec![
            RowTerm::Tuple(vec![RowTerm::Atom("b".into()), RowTerm::Integer(3),]),
            RowTerm::Tuple(vec![RowTerm::Atom("c".into()), RowTerm::Integer(4),]),
            RowTerm::Tuple(vec![RowTerm::Atom("e".into()), RowTerm::Integer(5),]),
        ])]
    );
    let rs = big_data.lookup_elem("1", RowTerm::Integer(2));
    assert_eq!(rs, vec![&RowTerm::Integer(90)]);
    let _ = big_data.append(
        get_rowdata(|| add_new_elem(|| get_term_key("e", 5), || RowTerm::Integer(100))),
        &option,
    );
    let rs = big_data.lookup_elem("1", RowTerm::Integer(2));
    assert_eq!(rs, vec![&RowTerm::Integer(100)]);
}
#[test]
fn only_update_location() {
    let mut big_data = BigData::new();
    let option = RowTerm::List(vec![
        // feature: update location
        RowTerm::Atom("location".into()),
        // pos = 0
        RowTerm::Tuple(vec![
            RowTerm::Integer(0),
            RowTerm::List(vec![RowTerm::Tuple(vec![
                RowTerm::Atom("update".into()),
                RowTerm::Atom("gt".into()),
            ])]),
        ]),
    ]);
    let _ = big_data.append(get_rowdata(|| get_term("a")), &option);
    let _ = big_data.append(get_rowdata(|| get_term("b")), &option);
    let _ = big_data.append(get_rowdata(|| get_term_location("c", 30)), &option);

    let rs = big_data.lookup_elem("1", RowTerm::Integer(1));
    assert_eq!(
        rs,
        vec![&RowTerm::List(vec![RowTerm::Tuple(vec![
            RowTerm::Atom("a".into()),
            RowTerm::Integer(3),
        ]),])]
    );
    let rs = big_data.lookup_elem("1", RowTerm::Integer(0));
    assert_eq!(rs, vec![&RowTerm::Integer(30)]);
    let _ = big_data.append(
        get_rowdata(|| add_new_elem(|| get_term_location("e", 50), || RowTerm::Integer(90))),
        &option,
    );
    let rs = big_data.lookup_elem("1", RowTerm::Integer(2));
    assert_eq!(rs, vec![&RowTerm::Integer(90)]);
    let rs = big_data.lookup_elem("1", RowTerm::Integer(0));
    assert_eq!(rs, vec![&RowTerm::Integer(50)]);
    let rs = big_data.lookup_elem("1", RowTerm::Integer(1));
    assert_eq!(
        rs,
        vec![&RowTerm::List(vec![RowTerm::Tuple(vec![
            RowTerm::Atom("a".into()),
            RowTerm::Integer(3),
        ]),])]
    );
}
#[test]
fn update_always() {
    let mut big_data = BigData::new();
    let option = RowTerm::List(vec![
        // feature: update location
        RowTerm::Atom("location".into()),
        // pos = 1
        RowTerm::Tuple(vec![
            RowTerm::Integer(1),
            RowTerm::List(vec![RowTerm::Tuple(vec![
                RowTerm::Atom("update".into()),
                RowTerm::Atom("always".into()),
            ])]),
        ]),
    ]);
    let _ = big_data.append(get_rowdata(|| get_term("a")), &option);
    let _ = big_data.append(get_rowdata(|| get_term("b")), &option);
    let rs = big_data.lookup_elem("1", RowTerm::Integer(1));
    assert_eq!(
        rs,
        vec![&RowTerm::List(vec![RowTerm::Tuple(vec![
            RowTerm::Atom("b".into()),
            RowTerm::Integer(3),
        ]),])]
    );
}
// =======
// construction functions for test
fn get_term(str: &str) -> RowTerm {
    RowTerm::Tuple(vec![
        RowTerm::Integer(0),
        RowTerm::List(vec![RowTerm::Tuple(vec![
            RowTerm::Atom(str.into()),
            RowTerm::Integer(3),
        ])]),
    ])
}
fn get_term_key(str: &str, key: i64) -> RowTerm {
    RowTerm::Tuple(vec![
        RowTerm::Integer(0),
        RowTerm::List(vec![RowTerm::Tuple(vec![
            RowTerm::Atom(str.into()),
            RowTerm::Integer(key),
        ])]),
    ])
}
fn get_term_location(str: &str, location: i64) -> RowTerm {
    RowTerm::Tuple(vec![
        RowTerm::Integer(location),
        RowTerm::List(vec![RowTerm::Tuple(vec![
            RowTerm::Atom(str.into()),
            RowTerm::Integer(3),
        ])]),
    ])
}
fn add_new_elem(f: fn() -> RowTerm, add: fn() -> RowTerm) -> RowTerm {
    let mut rs = vec![];
    if let RowTerm::Tuple(list) = f() {
        let mut temp = list.clone();
        rs.append(&mut temp);
        rs.push(add());
        RowTerm::Tuple(rs)
    } else {
        add()
    }
}
fn get_rowdata(f: fn() -> RowTerm) -> RowData {
    RowData::new("1", f(), 10)
}
