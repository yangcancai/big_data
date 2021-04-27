use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use rustler::{lazy_static::lazy::Lazy, resource::ResourceArc};
use rustler::{Binary, Encoder, Env, NifResult, OwnedBinary, Term};

use atoms::ok;
use options::Nifbig_dataOptions;
// =================================================================================================
// resource
// =================================================================================================
struct Nifbig_data {
    data: HashMap<String, String>,
}
impl Nifbig_data {
    // create
    fn new(_: Nifbig_dataOptions) -> Result<Self, String> {
        let a = HashMap::new();
        Ok(Nifbig_data { data: a })
    }
    // clear
    fn clear(&mut self) {
        self.data.clear();
    }
}
#[repr(transparent)]
struct Nifbig_dataResource(RwLock<Nifbig_data>);

impl Nifbig_dataResource {
    fn read(&self) -> RwLockReadGuard<'_, Nifbig_data> {
        self.0.read().unwrap()
    }

    fn write(&self) -> RwLockWriteGuard<'_, Nifbig_data> {
        self.0.write().unwrap()
    }
}

impl From<Nifbig_data> for Nifbig_dataResource {
    fn from(other: Nifbig_data) -> Self {
        Nifbig_dataResource(RwLock::new(other))
    }
}

pub fn on_load(env: Env, _load_info: Term) -> bool {
    rustler::resource!(Nifbig_dataResource, env);
    true
}

// =================================================================================================
// api
// =================================================================================================

#[rustler::nif]
fn new<'a>(env: Env<'a>, opts: Nifbig_dataOptions) -> NifResult<Term<'a>> {
    let rs = Nifbig_data::new(opts).map_err(|e| rustler::error::Error::Term(Box::new(e)))?;
    Ok((ok(), ResourceArc::new(Nifbig_dataResource::from(rs))).encode(env))
}
#[rustler::nif]
fn clear<'a>(env: Env<'a>, resource: ResourceArc<Nifbig_dataResource>) -> NifResult<Term<'a>> {
    resource.write().clear();
    Ok(ok().encode(env))
}
#[rustler::nif]
fn push<'a>(
    env: Env<'a>,
    resource: ResourceArc<Nifbig_dataResource>,
    msg: LazyBinary<'a>,
) -> NifResult<Term<'a>> {
    let _rs = resource.write();
    Ok(ok().encode(env))
}
#[rustler::nif]
fn pop<'a>(env: Env<'a>, resource: ResourceArc<Nifbig_dataResource>) -> NifResult<Term<'a>> {
    let _rs = resource.write();
    let p = true;
    Ok(p.encode(env))
}
// =================================================================================================
// helpers
// =================================================================================================

/// Represents either a borrowed `Binary` or `OwnedBinary`.
///
/// `LazyBinary` allows for the most efficient conversion from an
/// Erlang term to a byte slice. If the term is an actual Erlang
/// binary, constructing `LazyBinary` is essentially
/// zero-cost. However, if the term is any other Erlang type, it is
/// converted to an `OwnedBinary`, which requires a heap allocation.
enum LazyBinary<'a> {
    Owned(OwnedBinary),
    Borrowed(Binary<'a>),
}

impl<'a> std::ops::Deref for LazyBinary<'a> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Owned(owned) => owned.as_ref(),
            Self::Borrowed(borrowed) => borrowed.as_ref(),
        }
    }
}

impl<'a> rustler::Decoder<'a> for LazyBinary<'a> {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if term.is_binary() {
            Ok(Self::Borrowed(Binary::from_term(term)?))
        } else {
            Ok(Self::Owned(term.to_binary()))
        }
    }
}
