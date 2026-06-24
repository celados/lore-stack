// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use aws_sdk_dynamodb::types::CancellationReason;

/// A filter that only returns cancellation reasons that have a 'Code' that isn't the str literal "None".
/// AWS HTTP `CancellationReason` array can have dummy/blank reasons if some Items in a transaction were cancelled
/// because of other items in the transaction; not because of anything wrong with that particular item.
///
/// See <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CancellationReason.html>
pub fn interesting_cancellation_reason_filter(item: &&CancellationReason) -> bool {
    if let Some(code) = item.code()
        && code != "None"
    {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_code_is_not_interesting() {
        let reason = CancellationReason::builder().build();
        assert!(!interesting_cancellation_reason_filter(&&reason));
    }

    #[test]
    fn none_code_is_not_interesting() {
        let reason = CancellationReason::builder().code("None").build();
        assert!(!interesting_cancellation_reason_filter(&&reason));
    }

    #[test]
    fn a_code_is_interesting() {
        let reason = CancellationReason::builder().code("Hello").build();
        assert!(interesting_cancellation_reason_filter(&&reason));
    }
}
