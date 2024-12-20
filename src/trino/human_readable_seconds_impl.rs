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
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};
use humantime::format_duration;
use std::any::Any;
use std::time::Duration;

fn human_readable_seconds_double_invoke(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => {
            let duration = Duration::from_secs(*v as u64);
            let formatted_duration = format_duration(duration).to_string();
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                formatted_duration,
            ))))
        }
        _ => {
            internal_err!("Invalid argument types to human_readable_seconds function")
        }
    }
}

fn human_readable_seconds_double_return_type(_arg_types: &[DataType]) -> Result<DataType> {
    Ok(DataType::Utf8)
}

fn human_readable_seconds_double_simplify(
    args: Vec<Expr>,
    _info: &dyn SimplifyInfo,
) -> Result<ExprSimplifyResult> {
    Ok(ExprSimplifyResult::Original(args))
}

// ========== Generated template below this line ==========
// Do *NOT* edit below this line: all changes will be overwritten
// when template is regenerated!

#[derive(Debug)]
pub(super) struct human_readable_seconds_doubleFunc {
    signature: Signature,
}

impl human_readable_seconds_doubleFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for human_readable_seconds_doubleFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "human_readable_seconds"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        human_readable_seconds_double_return_type(arg_types)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        human_readable_seconds_double_invoke(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        human_readable_seconds_double_simplify(args, info)
    }
}
