// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

/// The outputs of multiple `QueryOutput`s combined
/// into 1 unified result. Partially mirrors the fields of `QueryOutput` for ease
/// of swapping `QueryOutput` for this type
#[derive(Default)]
pub struct QueryOutputAccumulation {
    pub items: Option<Vec<HashMap<String, AttributeValue>>>,
    pub count: i32,
}

impl QueryOutputAccumulation {
    // explicitly extend the results with fields found in a QueryOutput.
    // We do this rather than taking a `QueryOutput` itself so I can partially move
    // out other fields elsewhere as required (e.g. moving out the LastEvaluatedKey)
    pub fn extend(
        &mut self,
        mut new_items: Option<Vec<HashMap<String, AttributeValue>>>,
        count: i32,
    ) {
        if let Some(new_items) = new_items.as_mut() {
            let items = self
                .items
                .get_or_insert_with(|| Vec::with_capacity(new_items.len()));
            items.append(new_items);
        }
        self.count += count;
    }
}

#[cfg(test)]
impl From<aws_sdk_dynamodb::operation::query::QueryOutput> for QueryOutputAccumulation {
    fn from(value: aws_sdk_dynamodb::operation::query::QueryOutput) -> Self {
        let mut result = QueryOutputAccumulation::default();
        result.extend(value.items, value.count);
        result
    }
}
