// Copyright 2022 The template Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{error::Error, result::Result};

// pub const PROTOC: &str = env!("PROTOC");
// pub const PROTOC_INCLUDE: &str = env!("PROTOC_INCLUDE");

fn main() -> Result<(), Box<dyn Error>> {
    // std::env::set_var("PROTOC", PROTOC);
    // std::env::set_var("PROTOC_INCLUDE", PROTOC_INCLUDE);

    tonic_build::configure().compile(
        &[
            "proto/metadata.proto",
            "proto/templatekv.proto",
            "proto/node.proto",
            "proto/root.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}