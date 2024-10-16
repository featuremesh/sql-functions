// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![allow(non_camel_case_types)]
use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};
use regex::Regex;
use std::any::Any;
use std::sync::Arc;

use crate::utils_regexp::{
    map_rowfun__pat_hay_int_int_to_i64, map_rowfun__pat_hay_int_to_i64, map_rowfun__pat_hay_to_i64,
};

fn regexp_position_varchar_joniregexp_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    map_rowfun__pat_hay_to_i64(&args[1], &args[0], Arc::new(regexp_position__rowfun))
}

fn regexp_position__rowfun(pat: &str) -> Result<Arc<dyn Fn(/*hay:*/ &str) -> i64>> {
    let re = Regex::new(pat).map_err(|e| {
        DataFusionError::Execution(format!(
            "Regular expression for regexp_position did not compile: {e:?}"
        ))
    })?;
    let rowfun = move |str: &str| {
        let res = re.find(str);
        match res {
            None => -1,
            Some(m) => m.start() as i64 + 1,
        }
    };
    Ok(Arc::new(rowfun))
}

fn regexp_position_varchar_joniregexp_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Int64)
}

fn regexp_position_varchar_joniregexp_simplify(
    args: Vec<Expr>,
    _info: &dyn SimplifyInfo,
) -> Result<ExprSimplifyResult> {
    Ok(ExprSimplifyResult::Original(args))
}

fn regexp_position_varchar_joniregexp_bigint_invoke(
    args: &[ColumnarValue],
) -> Result<ColumnarValue> {
    map_rowfun__pat_hay_int_to_i64(
        &args[1],
        &args[0],
        &args[2],
        Arc::new(regexp_position__rowfun2),
    )
}

fn regexp_position__rowfun2(
    pat: &str,
) -> Result<Arc<dyn Fn(/*hay:*/ &str, /*start:*/ i64) -> i64>> {
    let re = Regex::new(pat).map_err(|e| {
        DataFusionError::Execution(format!(
            "Regular expression for regexp_position did not compile: {e:?}"
        ))
    })?;
    let rowfun = move |str: &str, start: i64| {
        let start = start as usize;
        if start < 1 {
            return -1; // TODO Trino gives out an error here; need to understand if Result from rowfun is worth it
        } else if start > str.len() {
            return -1;
            // Working around what might be a bug in regex crate: it panics here,
            // but only when start is "well past" the end of str
            // (e.g., for str="", must be start >= 3, for "abc" and pattern "b", start >= 6).
        }
        let res = re.find_at(str, start - 1);
        match res {
            None => -1,
            Some(m) => m.start() as i64 + 1,
        }
    };
    Ok(Arc::new(rowfun))
}

fn regexp_position_varchar_joniregexp_bigint_return_type(
    _arg_types: &[DataType],
) -> Result<DataType> {
    Ok(DataType::Int64)
}

fn regexp_position_varchar_joniregexp_bigint_simplify(
    args: Vec<Expr>,
    _info: &dyn SimplifyInfo,
) -> Result<ExprSimplifyResult> {
    Ok(ExprSimplifyResult::Original(args))
}

fn regexp_position_varchar_joniregexp_bigint_bigint_invoke(
    args: &[ColumnarValue],
) -> Result<ColumnarValue> {
    map_rowfun__pat_hay_int_int_to_i64(
        &args[1],
        &args[0],
        &args[2],
        &args[3],
        Arc::new(regexp_position__rowfun3),
    )
}

fn regexp_position__rowfun3(
    pat: &str,
) -> Result<Arc<dyn Fn(/*hay:*/ &str, /*start:*/ i64, /*occ:*/ i64) -> i64>> {
    let re = Regex::new(pat).map_err(|e| {
        DataFusionError::Execution(format!(
            "Regular expression for regexp_position did not compile: {e:?}"
        ))
    })?;
    let rowfun = move |str: &str, start: i64, occ: i64| {
        let start = start as usize;
        if start < 1 {
            return -1; // TODO Trino gives out an error here; need to understand if Result from rowfun is worth it
        } else if start > str.len() {
            return -1;
            // Working around what might be a bug in regex crate: it panics here,
            // but only when start is "well past" the end of str
            // (e.g., for str="", must be start >= 3, for "abc" and pattern "b", start >= 6).
        }
        if !str.is_char_boundary(start - 1) {
            return -1;
        };
        if occ < 1 {
            return -1; // TODO Trino gives out an error here as well
        }

        let suffix = &str[(start - 1)..];
        let res = re.find_iter(suffix).nth((occ - 1) as usize);
        match res {
            None => -1,
            Some(m) => (m.start() + 1) as i64 + (start - 1) as i64,
        }
    };
    Ok(Arc::new(rowfun))
}

fn regexp_position_varchar_joniregexp_bigint_bigint_return_type(
    _arg_types: &[DataType],
) -> Result<DataType> {
    Ok(DataType::Int64)
}

fn regexp_position_varchar_joniregexp_bigint_bigint_simplify(
    args: Vec<Expr>,
    _info: &dyn SimplifyInfo,
) -> Result<ExprSimplifyResult> {
    Ok(ExprSimplifyResult::Original(args))
}

// ========== Generated template below this line ==========
// Do *NOT* edit below this line: all changes will be overwritten
// when template is regenerated!

#[derive(Debug)]
pub(super) struct regexp_position_varchar_joniregexpFunc {
    signature: Signature,
}

impl regexp_position_varchar_joniregexpFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for regexp_position_varchar_joniregexpFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "regexp_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        regexp_position_varchar_joniregexp_return_type(arg_types)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        regexp_position_varchar_joniregexp_invoke(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        regexp_position_varchar_joniregexp_simplify(args, info)
    }
}

#[derive(Debug)]
pub(super) struct regexp_position_varchar_joniregexp_bigintFunc {
    signature: Signature,
}

impl regexp_position_varchar_joniregexp_bigintFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for regexp_position_varchar_joniregexp_bigintFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "regexp_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        regexp_position_varchar_joniregexp_bigint_return_type(arg_types)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        regexp_position_varchar_joniregexp_bigint_invoke(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        regexp_position_varchar_joniregexp_bigint_simplify(args, info)
    }
}

#[derive(Debug)]
pub(super) struct regexp_position_varchar_joniregexp_bigint_bigintFunc {
    signature: Signature,
}

impl regexp_position_varchar_joniregexp_bigint_bigintFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(4, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for regexp_position_varchar_joniregexp_bigint_bigintFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "regexp_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        regexp_position_varchar_joniregexp_bigint_bigint_return_type(arg_types)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        regexp_position_varchar_joniregexp_bigint_bigint_invoke(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        regexp_position_varchar_joniregexp_bigint_bigint_simplify(args, info)
    }
}
