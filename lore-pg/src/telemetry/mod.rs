// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Telemetry resource detectors for AWS environments.
//!
//! This module provides OpenTelemetry resource detectors that automatically
//! discover and report cloud/infrastructure metadata.

mod aws;

pub use aws::AWSResourceDetector;
