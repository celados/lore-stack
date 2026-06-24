// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::fmt::Debug;

use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum AwsError<E> {
    #[error("AWS SDK operation failed: {0:?}")]
    AwsSdkError(E),
    #[error("Dynamo BatchGetItem received empty keys")]
    MissingKeys,
    #[error("Failed to build batch request")]
    BatchRequestError,
    #[error("Failed to join task")]
    JoinError,
}
