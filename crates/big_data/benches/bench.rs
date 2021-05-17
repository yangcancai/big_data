#[macro_use]
extern crate bencher;
extern crate big_data;
extern crate rand;
use bencher::Bencher;
use big_data::big_data::{BigData, RowData, RowTerm};

const MAX: u128 = 10000;
fn gen_data() -> BigData {
    let mut big_data = BigData::new();
    for row_id in 0..MAX {
        let row = RowData::new(&row_id.to_string(), RowTerm::Integer(row_id as i64), row_id);
        big_data.insert(row);
    }
    big_data
}

fn get(bench: &mut Bencher) {
    let big_data = gen_data();
    bench.iter(|| {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let row_id: u128 = rng.gen_range(0..MAX);
        big_data.get(&row_id.to_string())
    })
}
fn insert(bench: &mut Bencher) {
    let mut big_data = gen_data();
    bench.iter(|| {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let row_id: u128 = rng.gen();
        let row = RowData::new(&row_id.to_string(), RowTerm::Integer(row_id as i64), row_id);
        big_data.insert(row);
    })
}
benchmark_group!(benches, insert, get);
benchmark_main!(benches);
