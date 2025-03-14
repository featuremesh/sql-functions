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

//! Helper functors for creating Regexp-based UDFs.
//! As with UDF functors in general, the idea is to have a generic implementation of
//! vectorization machinery that can accept a simple function defined over fields of a row
//! and map it over `ColumnarValue` inputs.
//! The functors here address Regexp specifics as follows:
//! - The case of the pattern column being a scalar is recognized and optimized for by compiling the
//!   pattern only once. (This case corresponds to a literal-string pattern in an SQL query.)
//! - Row functions are curried, with the first argument being a string for the regexp pattern.
//!   (This is different from the argument order in UDFs, but is better suited the regexp pre-compiling.)
//! - The pattern column (the first column) is assumed to be an SDF "distinct" type `joniregexp`
//!   and is handled suitably to extract the underlying string array.
//! Functor names are of the form map_rowfun__aaa_bbb_ccc_to_zzz,
//! where aaa, bbb, ccc, ... (up to `to`) indicate the types of the columns and of the row function's arguments,
//! while zzz indicates the output type.

#![allow(non_snake_case)]
use arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::common::cast::{as_int64_array, as_string_array};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

use crate::utils::{array_to_columnar, distinct_to_string_array};
use crate::utils_arrow::{ListArrayExt, StringArrayExt};

/// Map a curried row function that accepts a pattern and a string and returns an i64
/// over two columns, of jonigexp and haystack/string.  
pub(super) fn map_rowfun__pat_hay_to_i64<F>(
    joni_col: &ColumnarValue,
    hay_col: &ColumnarValue,
    rowfun: Arc<F>,
) -> Result<ColumnarValue>
where
    F: (Fn(/*pat:*/ &str) -> Result<Arc<dyn Fn(/*hay:*/ &str) -> i64>>) + Sync + Send + 'static,
{
    let res_arr = match joni_col {
        ColumnarValue::Array(joni_arr) => {
            let hay_arr = hay_col.to_owned().into_array(joni_arr.len())?;
            let pattern = distinct_to_string_array(joni_arr)?;
            let haystack = as_string_array(&hay_arr)?;
            let res = pattern
                .iter()
                .zip(haystack.iter())
                .map(|(pat_opt, hay_opt)| match (pat_opt, hay_opt) {
                    (Some(pat), Some(hay)) => {
                        let regfun = rowfun(pat)?;
                        Ok(Some(regfun(hay)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Int64Array>>()?;
            Arc::new(res) as ArrayRef
        }
        ColumnarValue::Scalar(joni_scalar) => {
            let hay_arr = hay_col.to_owned().into_array(1)?; // NB: 1 only triggers when hay_col is Scalar
            let joni_arr = joni_scalar.to_array()?;
            let pat_arr = distinct_to_string_array(&joni_arr)?;
            if pat_arr.is_null(0) {
                arrow::array::new_null_array(hay_arr.data_type(), hay_arr.len())
            } else {
                let pat = pat_arr.value(0);
                let regfun = rowfun(pat)?;
                let haystack = as_string_array(&hay_arr)?;
                let res = haystack
                    .iter()
                    .map(|hay_opt| hay_opt.map(|hay| regfun(hay)))
                    .collect::<Int64Array>();
                Arc::new(res) as ArrayRef
            }
        }
    };
    Ok(array_to_columnar(res_arr))
}

/// Map a curried row function that accepts a pattern, a string, and an integer and returns an i64
/// over three columns, of jonigexp, haystack/string, and integers.  
pub(super) fn map_rowfun__pat_hay_int_to_i64<F>(
    joni_col: &ColumnarValue,
    hay_col: &ColumnarValue,
    int1_col: &ColumnarValue,
    rowfun: Arc<F>,
) -> Result<ColumnarValue>
where
    F: (Fn(/*pat:*/ &str) -> Result<Arc<dyn Fn(/*hay:*/ &str, /*int1:*/ i64) -> i64>>)
        + Sync
        + Send
        + 'static,
{
    let res_arr = match joni_col {
        ColumnarValue::Array(joni_arr) => {
            let hay_arr = hay_col.to_owned().into_array(joni_arr.len())?;
            let int1_arr = int1_col.to_owned().into_array(joni_arr.len())?;
            let pattern = distinct_to_string_array(joni_arr)?;
            let haystack = as_string_array(&hay_arr)?;
            let int1 = as_int64_array(&int1_arr)?;
            let res = pattern
                .iter()
                .zip(haystack.iter())
                .zip(int1.iter())
                .map(|tuple| match tuple {
                    ((Some(pat), Some(hay)), Some(int1)) => {
                        let regfun = rowfun(pat)?;
                        Ok(Some(regfun(hay, int1)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Int64Array>>()?;
            Arc::new(res) as ArrayRef
        }
        ColumnarValue::Scalar(joni_scalar) => {
            let hay_arr = hay_col.to_owned().into_array(1)?; // NB: 1 only triggers when hay_col is Scalar
            let int1_arr = int1_col.to_owned().into_array(1)?;
            let joni_arr = joni_scalar.to_array()?;
            let pat_arr = distinct_to_string_array(&joni_arr)?;
            if pat_arr.is_null(0) {
                arrow::array::new_null_array(hay_arr.data_type(), hay_arr.len())
            } else {
                let pat = pat_arr.value(0);
                let regfun = rowfun(pat)?;
                let haystack = as_string_array(&hay_arr)?;
                let int1 = as_int64_array(&int1_arr)?;
                let res = haystack
                    .iter()
                    .zip(int1.iter())
                    .map(|tuple| match tuple {
                        (Some(hay), Some(int1)) => Some(regfun(hay, int1)),
                        _ => None,
                    })
                    .collect::<Int64Array>();
                Arc::new(res) as ArrayRef
            }
        }
    };
    Ok(array_to_columnar(res_arr))
}

pub(super) fn map_rowfun__pat_hay_int_int_to_i64<F>(
    joni_col: &ColumnarValue,
    hay_col: &ColumnarValue,
    int1_col: &ColumnarValue,
    int2_col: &ColumnarValue,
    rowfun: Arc<F>,
) -> Result<ColumnarValue>
where
    F: (Fn(
            /*pat:*/ &str,
        ) -> Result<Arc<dyn Fn(/*hay:*/ &str, /*int1:*/ i64, /*int2:*/ i64) -> i64>>)
        + Sync
        + Send
        + 'static,
{
    let res_arr = match joni_col {
        ColumnarValue::Array(joni_arr) => {
            let hay_arr = hay_col.to_owned().into_array(joni_arr.len())?;
            let int1_arr = int1_col.to_owned().into_array(joni_arr.len())?;
            let int2_arr = int2_col.to_owned().into_array(joni_arr.len())?;
            let pattern = distinct_to_string_array(joni_arr)?;
            let haystack = as_string_array(&hay_arr)?;
            let int1 = as_int64_array(&int1_arr)?;
            let int2 = as_int64_array(&int2_arr)?;
            let res = pattern
                .iter()
                .zip(haystack.iter())
                .zip(int1.iter())
                .zip(int2.iter())
                .map(|tuple| match tuple {
                    (((Some(pat), Some(hay)), Some(int1)), Some(int2)) => {
                        let regfun = rowfun(pat)?;
                        Ok(Some(regfun(hay, int1, int2)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Int64Array>>()?;
            Arc::new(res) as ArrayRef
        }
        ColumnarValue::Scalar(joni_scalar) => {
            let hay_arr = hay_col.to_owned().into_array(1)?; // NB: 1 only triggers when hay_col is Scalar
            let int1_arr = int1_col.to_owned().into_array(1)?;
            let int2_arr = int2_col.to_owned().into_array(1)?;
            let joni_arr = joni_scalar.to_array()?;
            let pat_arr = distinct_to_string_array(&joni_arr)?;
            if pat_arr.is_null(0) {
                arrow::array::new_null_array(hay_arr.data_type(), hay_arr.len())
            } else {
                let pat = pat_arr.value(0);
                let regfun = rowfun(pat)?;
                let haystack = as_string_array(&hay_arr)?;
                let int1 = as_int64_array(&int1_arr)?;
                let int2 = as_int64_array(&int2_arr)?;
                let res = haystack
                    .iter()
                    .zip(int1.iter())
                    .zip(int2.iter())
                    .map(|tuple| match tuple {
                        ((Some(hay), Some(int1)), Some(int2)) => Some(regfun(hay, int1, int2)),
                        _ => None,
                    })
                    .collect::<Int64Array>();
                Arc::new(res) as ArrayRef
            }
        }
    };
    Ok(array_to_columnar(res_arr))
}

// map_rowfun__pat_hay_int_to_nstr
/// Given a pattern-curried row function that returns a nullable string,
/// map it over columns, returning a column of strings.  
pub(super) fn map_rowfun__pat_hay_int_to_nstr<F>(
    joni_col: &ColumnarValue,
    hay_col: &ColumnarValue,
    int1_col: &ColumnarValue,
    rowfun: Arc<F>,
) -> Result<ColumnarValue>
where
    F: (Fn(
            /*pat:*/ &str,
        )
            -> Result<Arc<dyn for<'a> Fn(/*hay:*/ &'a str, /*int1:*/ i64) -> Option<&'a str>>>)
        + Sync
        + Send
        + 'static,
{
    let res_arr = match joni_col {
        ColumnarValue::Array(joni_arr) => {
            let hay_arr = hay_col.to_owned().into_array(joni_arr.len())?;
            let int1_arr = int1_col.to_owned().into_array(joni_arr.len())?;
            let pattern = distinct_to_string_array(joni_arr)?;
            let haystack = as_string_array(&hay_arr)?;
            let int1 = as_int64_array(&int1_arr)?;
            let res = pattern
                .iter()
                .zip(haystack.iter())
                .zip(int1.iter())
                .map(|tuple| match tuple {
                    ((Some(pat), Some(hay)), Some(int1)) => {
                        let regfun = rowfun(pat)?;
                        Ok(regfun(hay, int1))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<StringArrayExt>>()?;
            Arc::new(res.into_string_array()) as ArrayRef
        }
        ColumnarValue::Scalar(joni_scalar) => {
            let hay_arr = hay_col.to_owned().into_array(1)?; // NB: 1 only triggers when hay_col is Scalar
            let int1_arr = int1_col.to_owned().into_array(1)?;
            let joni_arr = joni_scalar.to_array()?;
            let pat_arr = distinct_to_string_array(&joni_arr)?;
            if pat_arr.is_null(0) {
                arrow::array::new_null_array(hay_arr.data_type(), hay_arr.len())
            } else {
                let pat = pat_arr.value(0);
                let regfun = rowfun(pat)?;
                let haystack = as_string_array(&hay_arr)?;
                let int1 = as_int64_array(&int1_arr)?;
                let res = haystack
                    .iter()
                    .zip(int1.iter())
                    .map(|tuple| match tuple {
                        (Some(hay), Some(int1)) => regfun(hay, int1),
                        _ => None,
                    })
                    .collect::<StringArrayExt>();
                Arc::new(res.into_string_array()) as ArrayRef
            }
        }
    };
    Ok(array_to_columnar(res_arr))
}

/// Map a pattern-curried row function that returns a string.
pub(super) fn map_rowfun__pat_hay_to_string<F>(
    joni_col: &ColumnarValue,
    hay_col: &ColumnarValue,
    rowfun: Arc<F>,
) -> Result<ColumnarValue>
where
    F: (Fn(/*pat:*/ &str) -> Result<Arc<dyn Fn(/*hay:*/ &str) -> String>>) + Sync + Send + 'static,
{
    let res_arr = match joni_col {
        ColumnarValue::Array(joni_arr) => {
            let hay_arr = hay_col.to_owned().into_array(joni_arr.len())?;
            let pattern = distinct_to_string_array(joni_arr)?;
            let haystack = as_string_array(&hay_arr)?;
            let res = pattern
                .iter()
                .zip(haystack.iter())
                .map(|(pat_opt, hay_opt)| match (pat_opt, hay_opt) {
                    (Some(pat), Some(hay)) => {
                        let regfun = rowfun(pat)?;
                        Ok(Some(regfun(hay)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<StringArrayExt>>()?;
            Arc::new(res.into_string_array()) as ArrayRef
        }
        ColumnarValue::Scalar(joni_scalar) => {
            let hay_arr = hay_col.to_owned().into_array(1)?; // NB: 1 only triggers when hay_col is Scalar
            let joni_arr = joni_scalar.to_array()?;
            let pat_arr = distinct_to_string_array(&joni_arr)?;
            if pat_arr.is_null(0) {
                arrow::array::new_null_array(hay_arr.data_type(), hay_arr.len())
            } else {
                let pat = pat_arr.value(0);
                let regfun = rowfun(pat)?;
                let haystack = as_string_array(&hay_arr)?;
                let res = haystack
                    .iter()
                    .map(|hay_opt| hay_opt.map(|hay| regfun(hay)))
                    .collect::<StringArrayExt>();
                Arc::new(res.into_string_array()) as ArrayRef
            }
        }
    };
    Ok(array_to_columnar(res_arr))
}

/// Map a pattern-curried row function that returns a string.
pub(super) fn map_rowfun__pat_hay_str_to_string<F>(
    joni_col: &ColumnarValue,
    hay_col: &ColumnarValue,
    str1_col: &ColumnarValue,
    rowfun: Arc<F>,
) -> Result<ColumnarValue>
where
    F: (Fn(/*pat:*/ &str) -> Result<Arc<dyn Fn(/*hay:*/ &str, /*str1:*/ &str) -> String>>)
        + Sync
        + Send
        + 'static,
{
    let res_arr = match joni_col {
        ColumnarValue::Array(joni_arr) => {
            let hay_arr = hay_col.to_owned().into_array(joni_arr.len())?;
            let str1_arr = str1_col.to_owned().into_array(joni_arr.len())?;
            let pattern = distinct_to_string_array(joni_arr)?;
            let haystack = as_string_array(&hay_arr)?;
            let str1 = as_string_array(&str1_arr)?;
            let res = pattern
                .iter()
                .zip(haystack.iter())
                .zip(str1.iter())
                .map(|tuple| match tuple {
                    ((Some(pat), Some(hay)), Some(str1)) => {
                        let regfun = rowfun(pat)?;
                        Ok(Some(regfun(hay, str1)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<StringArrayExt>>()?;
            Arc::new(res.into_string_array()) as ArrayRef
        }
        ColumnarValue::Scalar(joni_scalar) => {
            let hay_arr = hay_col.to_owned().into_array(1)?; // NB: 1 only triggers when hay_col is Scalar
            let str1_arr = str1_col.to_owned().into_array(1)?;
            let joni_arr = joni_scalar.to_array()?;
            let pat_arr = distinct_to_string_array(&joni_arr)?;
            if pat_arr.is_null(0) {
                arrow::array::new_null_array(hay_arr.data_type(), hay_arr.len())
            } else {
                let pat = pat_arr.value(0);
                let regfun = rowfun(pat)?;
                let haystack = as_string_array(&hay_arr)?;
                let str1 = as_string_array(&str1_arr)?;
                let res = haystack
                    .iter()
                    .zip(str1.iter())
                    .map(|tuple| match tuple {
                        (Some(hay), Some(str1)) => Some(regfun(hay, str1)),
                        _ => None,
                    })
                    .collect::<StringArrayExt>();
                Arc::new(res.into_string_array()) as ArrayRef
            }
        }
    };
    Ok(array_to_columnar(res_arr))
}

/// Given a pattern-curried row function that returns a Vec of non-nullable strings,
/// map it over columns, returning a column of string lists.  
pub(super) fn map_rowfun__pat_hay_to_strlst<F>(
    joni_col: &ColumnarValue,
    hay_col: &ColumnarValue,
    rowfun: Arc<F>,
) -> Result<ColumnarValue>
where
    F: (Fn(/*pat:*/ &str) -> Result<Arc<dyn for<'a> Fn(/*hay:*/ &'a str) -> Vec<&'a str>>>)
        + Sync
        + Send
        + 'static,
{
    let res_arr = match joni_col {
        ColumnarValue::Array(joni_arr) => {
            let hay_arr = hay_col.to_owned().into_array(joni_arr.len())?;
            let pattern = distinct_to_string_array(joni_arr)?;
            let haystack = as_string_array(&hay_arr)?;
            let res = pattern
                .iter()
                .zip(haystack.iter())
                .map(|(pat_opt, hay_opt)| match (pat_opt, hay_opt) {
                    (Some(pat), Some(hay)) => {
                        let regfun = rowfun(pat)?;
                        Ok(Some(regfun(hay)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<ListArrayExt>>()?;
            Arc::new(res.into_list_array()) as ArrayRef
        }
        ColumnarValue::Scalar(joni_scalar) => {
            let hay_arr = hay_col.to_owned().into_array(1)?; // NB: 1 only triggers when hay_col is Scalar
            let joni_arr = joni_scalar.to_array()?;
            let pat_arr = distinct_to_string_array(&joni_arr)?;
            if pat_arr.is_null(0) {
                arrow::array::new_null_array(hay_arr.data_type(), hay_arr.len())
            } else {
                let pat = pat_arr.value(0);
                let regfun = rowfun(pat)?;
                let haystack = as_string_array(&hay_arr)?;
                let res = haystack
                    .iter()
                    .map(|hay_opt| hay_opt.map(|hay| regfun(hay)))
                    .collect::<ListArrayExt>();
                Arc::new(res.into_list_array()) as ArrayRef
            }
        }
    };
    Ok(array_to_columnar(res_arr))
}

/// Given a pattern-curried row function that returns a Vec of nullable strings,
/// map it over columns, returning a column of string lists.  
pub(super) fn map_rowfun__pat_hay_int_to_nstrlst<F>(
    joni_col: &ColumnarValue,
    hay_col: &ColumnarValue,
    int1_col: &ColumnarValue,
    rowfun: Arc<F>,
) -> Result<ColumnarValue>
where
    F: (Fn(
            /*pat:*/ &str,
        )
            -> Result<Arc<dyn for<'a> Fn(/*hay:*/ &'a str, /*int1:*/ i64) -> Vec<Option<&'a str>>>>)
        + Sync
        + Send
        + 'static,
{
    let res_arr = match joni_col {
        ColumnarValue::Array(joni_arr) => {
            let hay_arr = hay_col.to_owned().into_array(joni_arr.len())?;
            let int1_arr = int1_col.to_owned().into_array(joni_arr.len())?;
            let pattern = distinct_to_string_array(joni_arr)?;
            let haystack = as_string_array(&hay_arr)?;
            let int1 = as_int64_array(&int1_arr)?;
            let res = pattern
                .iter()
                .zip(haystack.iter())
                .zip(int1.iter())
                .map(|tuple| match tuple {
                    ((Some(pat), Some(hay)), Some(int1)) => {
                        let regfun = rowfun(pat)?;
                        Ok(Some(regfun(hay, int1)))
                    }
                    _ => Ok(None),
                })
                .collect::<Result<ListArrayExt>>()?;
            Arc::new(res.into_list_array()) as ArrayRef
        }
        ColumnarValue::Scalar(joni_scalar) => {
            let hay_arr = hay_col.to_owned().into_array(1)?; // NB: 1 only triggers when hay_col is Scalar
            let int1_arr = int1_col.to_owned().into_array(1)?;
            let joni_arr = joni_scalar.to_array()?;
            let pat_arr = distinct_to_string_array(&joni_arr)?;
            if pat_arr.is_null(0) {
                arrow::array::new_null_array(hay_arr.data_type(), hay_arr.len())
            } else {
                let pat = pat_arr.value(0);
                let regfun = rowfun(pat)?;
                let haystack = as_string_array(&hay_arr)?;
                let int1 = as_int64_array(&int1_arr)?;
                let res = haystack
                    .iter()
                    .zip(int1.iter())
                    .map(|tuple| match tuple {
                        (Some(hay), Some(int1)) => Some(regfun(hay, int1)),
                        _ => None,
                    })
                    .collect::<ListArrayExt>();
                Arc::new(res.into_list_array()) as ArrayRef
            }
        }
    };
    Ok(array_to_columnar(res_arr))
}
