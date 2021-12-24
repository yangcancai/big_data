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

use core::big_data::BigData;
use redis_module::native_types::RedisType;
use redis_module::RedisError;
use redis_module::{raw, Context, NextArg, RedisResult, RedisString};
use std::os::raw::c_void;
static MY_REDIS_TYPE: RedisType = RedisType::new(
    "big_data1",
    0,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: None,
        rdb_save: None,
        aof_rewrite: None,
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

unsafe extern "C" fn free(value: *mut c_void) {
    Box::from_raw(value as *mut BigData);
}

fn big_data_set(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let two = args.next_arg()?;
    args.done()?;
    let binary: &[u8] = two.as_slice();
    let decoded = core::term::binary_to_term(binary);
    match decoded {
        Ok(row_data) => {
            ctx.log_debug(
                format!(
                    "key: {}, binary: {:?}, row_data: {:?}",
                    key, binary, row_data
                )
                .as_str(),
            );

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
            Ok("OK".into())
        }
        Err(e) => Err(RedisError::String(format!("ERR {:?}", e))),
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
            let r: Vec<core::big_data::RowData> = r.iter().map(|x| (*x).clone()).collect();
            let term = core::term::list_to_binary(r.as_slice());
            match term {
                Ok(term) => Ok(term.into()),
                Err(e) => Err(RedisError::String(format!("error:{:?}", e))),
            }
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
                let term = core::term::list_to_binary(&[r.clone()]);
                match term {
                    Ok(term) => Ok(term.into()),
                    Err(e) => Err(RedisError::String(format!("error:{:?}", e))),
                }
            } else {
                empty_list()
            }
        }
        None => empty_list(),
    }
}

fn empty_list() -> RedisResult {
    let r = core::term::list_to_binary(&[]);
    Ok(r.unwrap().into())
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
        ["big_data.get_row", big_data_get_row, "readonly", 1, 1, 1],
        ["big_data.get", big_data_get, "readonly", 1, 1, 1],
    ],
}
