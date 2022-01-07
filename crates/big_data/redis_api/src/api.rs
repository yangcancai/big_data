//-------------------------------------------------------------------
// @author yangcancai

// Copyright (c) 2021 by yangcancai(yangcancai0112@gmail.com), All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

// @doc
//
// @end
// Created : 2021-12-22T01:17:07+00:00
//-------------------------------------------------------------------

#![allow(clippy::all)]
use anyhow::Result;
use core::big_data::BigData;
use core::big_data::RowData;
use core::big_data::RowTerm;
use core::term::ErlRes;
use core::traits::FromBytes;
use core::traits::ToBytes;
use redis_module::native_types::RedisType;
use redis_module::RedisModuleString;
use redis_module::RedisModule_EmitAOF;
use redis_module::RedisModule_SaveStringBuffer;
use redis_module::{raw, Context, NextArg, RedisResult, RedisString};
use std::os::raw::c_char;
use std::os::raw::c_int;
use std::os::raw::c_void;
use std::ptr::null_mut;
static MY_REDIS_TYPE: RedisType = RedisType::new(
    "big_data1",
    0,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: Some(aof),
        free: Some(free),

        // Currently unused by Redis
        mem_usage: None,
        digest: None,

        // Aux data
        aux_load: None,
        aux_save: None,
        aux_save_triggers: 0,

        free_effort: None,
        unlink: None,
        copy: None,
        defrag: None,
    },
);
unsafe extern "C" fn rdb_load(rdb: *mut raw::RedisModuleIO, _encver: c_int) -> *mut c_void {
    let data = raw::load_string(rdb);
    match data {
        Ok(data) => {
            let rs = BigData::from_bytes(data.as_slice()).unwrap();
            Box::into_raw(Box::new(rs)) as *mut c_void
        }
        Err(_) => null_mut(),
    }
}
unsafe extern "C" fn rdb_save(rdb: *mut raw::RedisModuleIO, value: *mut c_void) {
    let v = &*(value as *mut BigData);
    match v.to_bytes() {
        Ok(v) => {
            RedisModule_SaveStringBuffer.unwrap()(rdb, v.as_ptr().cast::<c_char>(), v.len());
        }
        Err(e) => {
            println!("BigData Rdb save err: {:?}", e);
        }
    }
}
unsafe extern "C" fn aof(
    rdb: *mut raw::RedisModuleIO,
    key: *mut RedisModuleString,
    value: *mut c_void,
) {
    let v = &*(value as *mut BigData);
    let list = v.to_list();
    for row in list.into_iter() {
        match row.to_bytes() {
            Ok(b) => RedisModule_EmitAOF.unwrap()(
                rdb,
                "BIG_DATA.SET".as_ptr().cast::<c_char>(),
                "ss".as_ptr().cast::<c_char>(),
                key,
                b.as_ptr().cast::<c_char>(),
            ),
            Err(e) => {
                println!("BigData aof err: {:?}", e);
            }
        }
    }
}
unsafe extern "C" fn free(value: *mut c_void) {
    Box::from_raw(value as *mut BigData);
}
fn replicate_verbatim(ctx: &Context, is_ok: bool) {
    if is_ok {
        ctx.replicate_verbatim();
    }
}
/// Write begin here
///
fn big_data_update_elem(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let row_key = args.next_str()?;
    let elem_specs = args.next_arg()?;
    let elem_specs: &[u8] = elem_specs.as_slice();
    let rowterm = RowTerm::from_bytes(elem_specs);
    match rowterm {
        Ok(rowterm) => {
            let key = ctx.open_key_writable(&key);
            match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
                Some(value) => {
                    let rs = value.update_elem(row_key, rowterm).to_bytes();
                    replicate_verbatim(ctx, rs.is_ok());
                    to_redis_res(rs)
                }
                None => to_redis_res(ErlRes::NotFound.to_bytes()),
            }
        }
        Err(e) => error(format!("ERR {:?}", e)),
    }
}
fn big_data_update_counter(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let row_key = args.next_str()?;
    let elm_specs = args.next_arg()?;
    let elm_specs: &[u8] = elm_specs.as_slice();
    let row_term = RowTerm::from_bytes(elm_specs);
    match row_term {
        Ok(row_term) => {
            let key = ctx.open_key_writable(&key);
            match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
                Some(value) => {
                    let rs = value.update_counter(row_key, row_term).to_bytes();
                    replicate_verbatim(ctx, rs.is_ok());
                    to_redis_res(rs)
                }
                None => to_redis_res(ErlRes::NotFound.to_bytes()),
            }
        }
        Err(e) => error(format!("ERR: {:?}", e)),
    }
}
fn big_data_set(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let two = args.next_arg()?;
    args.done()?;
    let binary: &[u8] = two.as_slice();
    let decoded = Vec::<RowData>::from_bytes(binary);
    match decoded {
        Ok(row_data) => {
            // ctx.log_debug(
            // format!(
            // "key: {}, binary: {:?}, row_data: {:?}",
            // key, binary, row_data
            // )
            // .as_str(),
            // );

            let key = ctx.open_key_writable(&key);

            match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
                Some(value) => {
                    value.insert_list(row_data);
                }
                None => {
                    let mut big_data = BigData::new();
                    big_data.insert_list(row_data);
                    key.set_value(&MY_REDIS_TYPE, big_data)?;
                }
            }
            // This function will replicate the command exactly as it was invoked by the client.
            ctx.replicate_verbatim();
            to_redis_res_ok()
        }
        Err(e) => {
            ctx.log_debug(format!("Error: {:?}", e).as_str());
            error(format!("ERR {:?}", e))
        }
    }
}
/// Remove begin here
///
fn big_data_remove_row(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let row_id = args.next_str()?;
    args.done()?;
    let key = ctx.open_key_writable(&key);
    match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
        Some(value) => {
            value.remove(row_id);
            ctx.replicate_verbatim();
            to_redis_res_ok()
        }
        None => to_redis_res_ok(),
    }
}
fn big_data_remove(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let key = ctx.open_key_writable(&key);
    match key.delete() {
        Ok(_ok) => {
            ctx.replicate_verbatim();
            to_redis_res_ok()
        }
        Err(e) => Err(e),
    }
}
fn big_data_remove_row_ids(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let start_time = args.next_u64()?;
    let end_time = args.next_u64()?;
    let key = ctx.open_key_writable(&key);
    match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
        Some(value) => {
            value.remove_row_ids(start_time as u128, end_time as u128);
            ctx.replicate_verbatim();
            to_redis_res_ok()
        }
        None => to_redis_res_ok(),
    }
}
/// Read begin here
///
fn big_data_lookup_elem(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let row_key = args.next_str()?;
    let elem_specs = args.next_arg()?;
    let elem_specs = elem_specs.as_slice();
    args.done()?;
    let rowterm = RowTerm::from_bytes(elem_specs);
    match rowterm {
        Ok(rowterm) => {
            let key = ctx.open_key(&key);
            match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
                Some(value) => {
                    let row_list = value.lookup_elem(row_key, rowterm.clone());
                    ctx.log_debug(
                        format!(
                            "lookup_elem key: {},row_term:{:?}, row_list: {:?}: binary:{:?}",
                            row_key, rowterm, row_list, elem_specs
                        )
                        .as_str(),
                    );

                    to_redis_res(row_list.to_bytes())
                }
                None => to_redis_res(ErlRes::NotFound.to_bytes()),
            }
        }
        Err(e) => error(format!("ERR {:?}", e)),
    }
}
fn big_data_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let key = ctx.open_key(&key);
    args.done()?;
    match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
        Some(value) => {
            let r = value.to_list();
            to_redis_res(r.to_bytes())
        }
        None => empty_list(),
    }
}

fn big_data_get_row(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let row_key = args.next_str()?;
    let key = ctx.open_key(&key);
    args.done()?;
    match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
        Some(value) => {
            if let Some(r) = value.get(row_key) {
                let term = &[r.clone()];
                to_redis_res(term.to_bytes())
            } else {
                empty_list()
            }
        }
        None => empty_list(),
    }
}
fn big_data_get_range(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let start_time = args.next_u64()?;
    let end_time = args.next_u64()?;
    let key = ctx.open_key(&key);
    args.done()?;
    match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
        Some(value) => to_redis_res(
            value
                .get_range(start_time as u128, end_time as u128)
                .to_bytes(),
        ),
        None => empty_list(),
    }
}
fn big_data_get_range_row_ids(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let start_time = args.next_u64()?;
    let end_time = args.next_u64()?;
    let key = ctx.open_key(&key);
    args.done()?;
    match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
        Some(value) => to_redis_res(
            value
                .get_range_row_ids(start_time as u128, end_time as u128)
                .to_bytes(),
        ),
        None => empty_list(),
    }
}
fn big_data_get_row_ids(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let time = args.next_u64()?;
    let key = ctx.open_key(&key);
    args.done()?;
    match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
        Some(value) => to_redis_res(value.get_row_ids(time as u128).to_bytes()),
        None => empty_list(),
    }
}
fn big_data_get_time_index(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let row_id = args.next_str()?;
    let key = ctx.open_key(&key);
    args.done()?;
    match key.get_value::<BigData>(&MY_REDIS_TYPE)? {
        Some(value) => to_redis_res(value.get_time_index(row_id).to_bytes()),
        None => empty_list(),
    }
}
fn empty_list() -> RedisResult {
    let r: &[RowData] = &[];
    Ok(r.to_bytes().unwrap().into())
}
fn error(str: String) -> RedisResult {
    to_redis_res(ErlRes::ErrString(str).to_bytes())
}
fn to_redis_res_ok() -> RedisResult {
    to_redis_res(ErlRes::Ok.to_bytes())
}
fn to_redis_res(res: Result<Vec<u8>>) -> RedisResult {
    match res {
        Ok(binary) => Ok(binary.into()),
        Err(e) => error(format!("ERR: {:?}", e)),
    }
}
//////////////////////////////////////////////////////
redis_module! {
    name: "big_data",
    version: 1,
    data_types: [
        MY_REDIS_TYPE,
    ],
    commands: [
        ["big_data.set", big_data_set, "write", 1, 1, 1],
        ["big_data.update_elem", big_data_update_elem, "write", 1, 1, 1],
        ["big_data.update_counter", big_data_update_counter, "write", 1, 1, 1],
        ["big_data.remove", big_data_remove, "write", 1, 1, 1],
        ["big_data.remove_row", big_data_remove_row, "write", 1, 1, 1],
        ["big_data.remove_row_ids", big_data_remove_row_ids, "write", 1, 1, 1],
        ["big_data.get_row", big_data_get_row, "readonly", 1, 1, 1],
        ["big_data.get", big_data_get, "readonly", 1, 1, 1],
        ["big_data.get_range", big_data_get_range, "readonly", 1, 1, 1],
        ["big_data.get_range_row_ids", big_data_get_range_row_ids, "readonly", 1, 1, 1],
        ["big_data.get_row_ids", big_data_get_row_ids, "readonly", 1, 1, 1],
        ["big_data.get_time_index", big_data_get_time_index, "readonly", 1, 1, 1],
        ["big_data.lookup_elem", big_data_lookup_elem, "readonly", 1, 1, 1],
    ],
}
