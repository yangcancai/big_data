use rustler::{Decoder, NifResult, Term};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Clone, Copy, Debug)]
pub struct Nifbig_dataOptions {
    pub bitmap_size: Option<usize>,
    pub items_count: Option<usize>,
    pub capacity: Option<usize>,
    pub rotate_at: Option<usize>,
    pub fp_rate: Option<f64>,
}

impl Default for Nifbig_dataOptions {
    fn default() -> Nifbig_dataOptions {
        Nifbig_dataOptions {
            bitmap_size: None,
            items_count: None,
            capacity: None,
            rotate_at: None,
            fp_rate: None,
        }
    }
}

impl<'a> Decoder<'a> for Nifbig_dataOptions {
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
