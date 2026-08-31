// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

use std::fmt::Debug;
use std::time::Duration;

use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use aws_types::request_id::RequestId;
use lore_telemetry::LabelArray;
use lore_telemetry::observe::observe_result;
use opentelemetry::KeyValue;
use tracing::warn;

pub mod aws_error;
pub mod s3;
pub mod store;
pub mod tls;

pub type SdkError<E> = ::aws_smithy_runtime_api::client::result::SdkError<
    E,
    ::aws_smithy_runtime_api::client::orchestrator::HttpResponse,
>;
pub type AwsResult<T, E> = Result<T, SdkError<E>>;

const REQUEST_ID_FIELD: &str = "request_id";
const ELAPSED_MS_FIELD: &str = "elapsed_ms";
const AWS_ERROR_CODE_LABEL_KEY: &str = "aws_code";

pub fn observe_aws_operation_callback<T, E>(
    slow_threshold: Duration,
) -> impl Fn(&AwsResult<T, E>, &Duration, &mut LabelArray) + Copy
where
    T: RequestId + Debug,
    E: RequestId + ProvideErrorMetadata + Debug,
{
    move |result: &AwsResult<T, E>, elapsed: &Duration, labels: &mut LabelArray| {
        observe_result(result, elapsed, labels);

        if let Err(error) = result {
            let label = if let Some(code) = error.code() {
                KeyValue::new(AWS_ERROR_CODE_LABEL_KEY, code.to_string())
            } else {
                warn!(
                    {ELAPSED_MS_FIELD} = elapsed.as_millis(),
                    {REQUEST_ID_FIELD} = error.request_id(),
                    error = ?error,
                    "AWS non-ServiceError"
                );
                let kind = match error {
                    SdkError::TimeoutError(_) => "SdkTimeoutError",
                    SdkError::DispatchFailure(dispatch) if dispatch.is_io() => "DispatchFailure_IO",
                    SdkError::DispatchFailure(dispatch) if dispatch.is_timeout() => {
                        "DispatchFailure_Timeout"
                    }
                    SdkError::DispatchFailure(dispatch) if dispatch.is_other() => {
                        "DispatchFailure_Other"
                    }
                    SdkError::DispatchFailure(_) => "DispatchFailure",
                    _ => "<unknown>",
                };
                KeyValue::new(AWS_ERROR_CODE_LABEL_KEY, kind)
            };
            labels.push(label);
        }

        let is_slow = *elapsed > slow_threshold;
        if is_slow {
            let request_id = match result {
                Ok(output) => output.request_id(),
                Err(error) => error.request_id(),
            }
            .unwrap_or("<unknown>");
            warn!(
                {ELAPSED_MS_FIELD} = elapsed.as_millis(),
                slow_threshold = ?slow_threshold,
                {REQUEST_ID_FIELD} = request_id,
                "Operation execution time exceeded slow operation threshold"
            );
        }
        labels.push(KeyValue::new("slow", is_slow));
    }
}
