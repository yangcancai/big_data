use rustler::{Decoder, NifResult, Term};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, PartialEq, Clone, Copy, Debug)]
pub struct NifBigDataOptions {
    pub bitmap_size: Option<usize>,
    pub items_count: Option<usize>,
    pub capacity: Option<usize>,
    pub rotate_at: Option<usize>,
    pub fp_rate: Option<f64>,
}

impl<'a> Decoder<'a> for NifBigDataOptions {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let mut opts = Self::default();
        use rustler::{Error, MapIterator};
        for (key, value) in MapIterator::new(term).ok_or(Error::BadArg)? {
            match key.atom_to_string()?.as_ref() {
                "bitmap_size" => opts.bitmap_size = Some(value.decode()?),
                "items_count" => opts.items_count = Some(value.decode()?),
                "capacity" => opts.capacity = Some(value.decode()?),
                "rotate_at" => opts.rotate_at = Some(value.decode()?),
                "fp_rate" => opts.fp_rate = Some(value.decode()?),
                _ => (),
            }
        }
        Ok(opts)
    }
}
