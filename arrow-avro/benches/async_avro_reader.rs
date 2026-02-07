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

//! Benchmarks for AsyncAvroFileReader

use arrow_array::RecordBatch;
use arrow_avro::reader::AsyncAvroFileReader;
use arrow_avro::schema::AvroSchema;
use criterion::{Criterion, criterion_group, criterion_main};
use futures::TryStreamExt;
use std::path::Path;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::runtime;

const TEST_FILE: &str = "test/data/network-device-events-0.avro";

const BATCH_SIZE: usize = 8192;

const READER_SCHEMA_NULLABLE: &str = r#"
    {
        "type": "record",
        "name": "NetworkDeviceEvent",
        "fields": [
            {
                "name": "timestamp",
                "type": [
                    "null",
                    {
                        "type": "record",
                        "name": "Timestamp",
                        "fields": [
                            {
                                "name": "seconds",
                                "type": ["null", "long"]
                            },
                            {
                                "name": "nanos",
                                "type": ["null", "int"]
                            }
                        ]
                    }
                ]
            }
        ]
    }
"#;

const READER_SCHEMA_NON_NULL: &str = r#"
    {
        "type": "record",
        "name": "NetworkDeviceEvent",
        "fields": [
            {
                "name": "timestamp",
                "type": {
                    "type": "record",
                    "name": "Timestamp",
                    "fields": [
                        {
                            "name": "seconds",
                            "type": "long"
                        },
                        {
                            "name": "nanos",
                            "type": "int"
                        }
                    ]
                }
            }
        ]
    }
"#;

async fn read_avro_file(
    path: &Path,
    batch_size: usize,
    reader_schema: AvroSchema,
) -> Vec<RecordBatch> {
    let file = File::open(path).await.unwrap();
    let file_size = file.metadata().await.unwrap().len();
    let buf_reader = BufReader::with_capacity(1024 * 1024, file);

    let reader = AsyncAvroFileReader::builder(buf_reader, file_size, batch_size)
        .with_reader_schema(reader_schema)
        .try_build()
        .await
        .unwrap();
    reader.try_collect().await.unwrap()
}

fn bench_async_avro_reader(c: &mut Criterion) {
    let rt = runtime::Builder::new_current_thread().build().unwrap();

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(TEST_FILE);
    if !path.exists() {
        panic!("Test file not found: {}", path.display());
    }

    let mut group = c.benchmark_group("async_avro_reader");

    let reader_schema = AvroSchema::new(READER_SCHEMA_NULLABLE.into());
    group.bench_function("project_nullable", |b| {
        b.to_async(&rt)
            .iter_with_large_drop(|| read_avro_file(&path, BATCH_SIZE, reader_schema.clone()));
    });

    let reader_schema = AvroSchema::new(READER_SCHEMA_NON_NULL.into());
    group.bench_function("project_non_null", |b| {
        b.to_async(&rt)
            .iter_with_large_drop(|| read_avro_file(&path, BATCH_SIZE, reader_schema.clone()));
    });

    group.finish();
}

criterion_group!(benches, bench_async_avro_reader);
criterion_main!(benches);
