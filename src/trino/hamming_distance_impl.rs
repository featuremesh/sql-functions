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
use crate::utils::make_scalar_function;
use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, OffsetSizeTrait};
use arrow::datatypes::DataType;
use datafusion::common::cast::as_generic_string_array;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

fn hamming<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!(
            "hamming function requires two arguments, got {}",
            args.len()
        );
    }

    let str1_array = as_generic_string_array::<T>(&args[0])?;
    let str2_array = as_generic_string_array::<T>(&args[1])?;

    match args[0].data_type() {
        DataType::Utf8View | DataType::Utf8 => {
            let result = str1_array
                .iter()
                .zip(str2_array.iter())
                .map(|(string1, string2)| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        Some(hamming_distance(string1, string2) as i32)
                    }
                    _ => None,
                })
                .collect::<Int32Array>();
            Ok(Arc::new(result) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let result = str1_array
                .iter()
                .zip(str2_array.iter())
                .map(|(string1, string2)| match (string1, string2) {
                    (Some(string1), Some(string2)) => {
                        Some(hamming_distance(string1, string2) as i64)
                    }
                    _ => None,
                })
                .collect::<Int64Array>();
            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
                "hamming was called with {other} datatype arguments. It requires Utf8View, Utf8 or LargeUtf8."
            )
        }
    }
}

fn hamming_distance(s1: &str, s2: &str) -> usize {
    s1.chars().zip(s2.chars()).filter(|(c1, c2)| c1 != c2).count()
}


fn hamming_distance_varchar_varchar_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match args[0].data_type() {
        DataType::Utf8View | DataType::Utf8 => make_scalar_function(hamming::<i32>, vec![])(args),
        DataType::LargeUtf8 => make_scalar_function(hamming::<i64>, vec![])(args),
        other => exec_err!("Unsupported data type {other:?} for function hamming")

    }
}

fn hamming_distance_varchar_varchar_return_type(arg_types: &[DataType]) -> Result<DataType> {
    match &arg_types[0] {
        DataType::Utf8View | DataType::Utf8 => Ok(DataType::Int32),
        DataType::LargeUtf8 => Ok(DataType::Int64),
        other => exec_err!("Unsupported data type {other:?} for function hamming")
    }
}

fn hamming_distance_varchar_varchar_simplify(
    args: Vec<Expr>,
    _info: &dyn SimplifyInfo,
) -> Result<ExprSimplifyResult> {
    Ok(ExprSimplifyResult::Original(args))
}

// ========== Generated template below this line ==========
// Do *NOT* edit below this line: all changes will be overwritten
// when template is regenerated!

#[derive(Debug)]
pub(super) struct hamming_distance_varchar_varcharFunc {
    signature: Signature,
}

impl hamming_distance_varchar_varcharFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for hamming_distance_varchar_varcharFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "hamming_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        hamming_distance_varchar_varchar_return_type(arg_types)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        hamming_distance_varchar_varchar_invoke(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        hamming_distance_varchar_varchar_simplify(args, info)
    }
}
