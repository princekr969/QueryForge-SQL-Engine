use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tonic::Status;

use crate::dataforge::{AggregationGroup, PartialResult, TaskRequest};
use crate::object_store::download_partition;

fn memory_limit_bytes() -> usize {
    let raw = std::env::var("WORKER_MEMORY_LIMIT").unwrap_or_else(|_| "256MB".to_owned());
    let upper = raw.trim().to_ascii_uppercase();
    for (suffix, multiplier) in [
        ("GB", 1024usize.pow(3)),
        ("MB", 1024usize.pow(2)),
        ("KB", 1024usize),
    ] {
        if let Some(number) = upper.strip_suffix(suffix) {
            return number.trim().parse::<usize>().unwrap_or(256) * multiplier;
        }
    }
    upper.parse().unwrap_or(256 * 1024 * 1024)
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn where_clause(request: &TaskRequest) -> String {
    if request.predicates.is_empty() {
        return String::new();
    }
    let predicates = request
        .predicates
        .iter()
        .map(|predicate| {
            let value = if predicate.r#type == "number" {
                predicate.value.parse::<f64>().unwrap().to_string()
            } else {
                quote_literal(&predicate.value)
            };
            format!(
                "{} {} {}",
                quote_identifier(&predicate.column),
                predicate.operator,
                value
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(" WHERE {predicates}")
}

fn build_sql(request: &TaskRequest) -> (String, bool) {
    let aggregated = !request.group_by_columns.is_empty() || !request.aggregations.is_empty();
    if !aggregated {
        let projection = if request.select_columns.is_empty()
            || request.select_columns.first().map(String::as_str) == Some("*")
        {
            "*".to_owned()
        } else {
            request
                .select_columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        };
        return (
            format!("SELECT {projection} FROM data{}", where_clause(request)),
            false,
        );
    }

    let mut projections = request
        .group_by_columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>();
    projections.push("COUNT(*) AS \"__rows\"".to_owned());
    for (index, aggregation) in request.aggregations.iter().enumerate() {
        let column = if aggregation.column == "*" {
            "*".to_owned()
        } else {
            quote_identifier(&aggregation.column)
        };
        match aggregation.function.as_str() {
            "COUNT" => projections.push(format!("COUNT({column}) AS \"__c{index}\"")),
            "AVG" => {
                projections.push(format!("SUM({column}) AS \"__v{index}\""));
                projections.push(format!("COUNT({column}) AS \"__c{index}\""));
            }
            function => {
                projections.push(format!("{function}({column}) AS \"__v{index}\""));
                projections.push(format!("COUNT({column}) AS \"__c{index}\""));
            }
        }
    }
    let groups = if request.group_by_columns.is_empty() {
        String::new()
    } else {
        format!(
            " GROUP BY {}",
            request
                .group_by_columns
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    (
        format!(
            "SELECT {} FROM data{}{}",
            projections.join(", "),
            where_clause(request),
            groups
        ),
        true,
    )
}

fn batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    // The established worker contract normalizes projected values to strings.
    // Keeping that boundary stable prevents JS Arrow readers from exposing
    // Int64 values as BigInt and preserves interchangeable worker behavior.
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), DataType::Utf8, false))
        .collect::<Vec<_>>();
    let columns = (0..batch.num_columns())
        .map(|column| {
            let source = batch.column(column);
            let values = (0..batch.num_rows())
                .map(|row| {
                    if source.is_null(row) {
                        String::new()
                    } else {
                        array_value_to_string(source.as_ref(), row).unwrap_or_default()
                    }
                })
                .collect::<Vec<_>>();
            Arc::new(StringArray::from(values)) as ArrayRef
        })
        .collect::<Vec<_>>();
    let normalized = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
    let mut output = Cursor::new(Vec::new());
    {
        let mut writer = StreamWriter::try_new(&mut output, &normalized.schema())?;
        writer.write(&normalized)?;
        writer.finish()?;
    }
    Ok(output.into_inner())
}

fn value(batch: &RecordBatch, column: &str, row: usize) -> Option<String> {
    let index = batch.schema().index_of(column).ok()?;
    let array = batch.column(index);
    if array.is_null(row) {
        None
    } else {
        array_value_to_string(array.as_ref(), row).ok()
    }
}

fn aggregation_groups(batch: &RecordBatch, request: &TaskRequest) -> Vec<AggregationGroup> {
    (0..batch.num_rows())
        .map(|row| {
            let group_values = request
                .group_by_columns
                .iter()
                .map(|column| {
                    (
                        column.clone(),
                        value(batch, column, row).unwrap_or_default(),
                    )
                })
                .collect::<HashMap<_, _>>();
            let group_key = serde_json::to_string(
                &request
                    .group_by_columns
                    .iter()
                    .map(|column| group_values.get(column).cloned().unwrap_or_default())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            let mut values = HashMap::new();
            let mut counts = HashMap::new();
            for (index, aggregation) in request.aggregations.iter().enumerate() {
                counts.insert(
                    aggregation.alias.clone(),
                    value(batch, &format!("__c{index}"), row)
                        .and_then(|item| item.parse().ok())
                        .unwrap_or(0),
                );
                if aggregation.function != "COUNT" {
                    if let Some(number) =
                        value(batch, &format!("__v{index}"), row).and_then(|item| item.parse().ok())
                    {
                        values.insert(aggregation.alias.clone(), number);
                    }
                }
            }
            AggregationGroup {
                group_key,
                count: value(batch, "__rows", row)
                    .and_then(|item| item.parse().ok())
                    .unwrap_or(0),
                sums: HashMap::new(),
                group_values,
                values,
                counts,
            }
        })
        .collect()
}

fn pruning_metrics(request: &TaskRequest) -> (i64, i64) {
    let Ok(stats) = serde_json::from_str::<Value>(&request.partition_stats_json) else {
        return (request.partition_byte_size, 0);
    };
    let Some(groups) = stats.get("rowGroups").and_then(Value::as_array) else {
        return (request.partition_byte_size, 0);
    };
    let mut total = 0;
    let mut skipped = 0;
    for group in groups {
        let bytes = group
            .get("compressedBytes")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        total += bytes;
        let impossible = request.predicates.iter().any(|predicate| {
            let Some(column) = group.get("columns").and_then(|v| v.get(&predicate.column)) else {
                return false;
            };
            let (Some(minimum), Some(maximum)) = (column.get("min"), column.get("max")) else {
                return false;
            };
            if minimum.is_null() || maximum.is_null() {
                return false;
            }
            if predicate.r#type == "number" {
                let min = minimum.as_str().and_then(|v| v.parse::<f64>().ok());
                let max = maximum.as_str().and_then(|v| v.parse::<f64>().ok());
                let target = predicate.value.parse::<f64>().ok();
                match (min, max, target) {
                    (Some(min), Some(max), Some(target)) => match predicate.operator.as_str() {
                        ">" => max <= target,
                        ">=" => max < target,
                        "<" => min >= target,
                        "<=" => min > target,
                        "=" => target < min || target > max,
                        "!=" | "<>" => min == target && max == target,
                        _ => false,
                    },
                    _ => false,
                }
            } else {
                let min = minimum.as_str().unwrap_or_default();
                let max = maximum.as_str().unwrap_or_default();
                let target = predicate.value.as_str();
                match predicate.operator.as_str() {
                    ">" => max <= target,
                    ">=" => max < target,
                    "<" => min >= target,
                    "<=" => min > target,
                    "=" => target < min || target > max,
                    "!=" | "<>" => min == target && max == target,
                    _ => false,
                }
            }
        });
        if impossible {
            skipped += bytes;
        }
    }
    (total - skipped, skipped)
}

async fn send_result(
    sender: &mpsc::Sender<Result<PartialResult, Status>>,
    result: PartialResult,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sender
        .send(Ok(result))
        .await
        .map_err(|_| "client cancelled result stream".into())
}

fn primary_object_keys(request: &TaskRequest) -> Vec<String> {
    if request.primary_partition_paths.is_empty() {
        vec![request.partition_path.clone()]
    } else {
        request.primary_partition_paths.clone()
    }
}

pub async fn execute(
    request: TaskRequest,
    sender: mpsc::Sender<Result<PartialResult, Status>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if request.storage_format != "parquet" {
        return Err("DataFusion worker accepts committed Parquet snapshots only".into());
    }
    let temporary = TempDir::new()?;
    let parquet_path = temporary.path().join("partition.parquet");
    if request.execution_sql.is_empty() || request.primary_partition_paths.is_empty() {
        download_partition(&request.partition_path, &parquet_path).await?;
    }

    let config = SessionConfig::new()
        .with_target_partitions(2)
        .with_batch_size(8192);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(memory_limit_bytes())))
        .with_temp_file_path(temporary.path().join("spill"))
        .build()?;
    let context = SessionContext::new_with_config_rt(config, Arc::new(runtime));
    if request.execution_sql.is_empty() {
        context
            .register_parquet(
                "data",
                parquet_path.to_string_lossy().as_ref(),
                ParquetReadOptions::default(),
            )
            .await?;
    } else {
        if request.primary_table_alias.is_empty() || request.secondary_table_alias.is_empty() {
            return Err("join task is missing table aliases".into());
        }
        let primary_location = if request.primary_partition_paths.is_empty() {
            parquet_path.clone()
        } else {
            let probe_directory = temporary.path().join("probe");
            tokio::fs::create_dir_all(&probe_directory).await?;
            for (index, object_key) in primary_object_keys(&request).iter().enumerate() {
                let destination = probe_directory.join(format!("partition-{index}.parquet"));
                download_partition(object_key, &destination).await?;
            }
            probe_directory
        };
        context
            .register_parquet(
                &request.primary_table_alias,
                primary_location.to_string_lossy().as_ref(),
                ParquetReadOptions::default(),
            )
            .await?;
        let build_directory = temporary.path().join("build");
        tokio::fs::create_dir_all(&build_directory).await?;
        for (index, object_key) in request.secondary_partition_paths.iter().enumerate() {
            let destination = build_directory.join(format!("partition-{index}.parquet"));
            download_partition(object_key, &destination).await?;
        }
        context
            .register_parquet(
                &request.secondary_table_alias,
                build_directory.to_string_lossy().as_ref(),
                ParquetReadOptions::default(),
            )
            .await?;
    }
    let (sql, aggregated) = if request.execution_sql.is_empty() {
        build_sql(&request)
    } else {
        (
            request.execution_sql.clone(),
            !request.group_by_columns.is_empty() || !request.aggregations.is_empty(),
        )
    };
    let dataframe = context.sql(&sql).await?;
    let output_schema = dataframe.schema().inner().clone();
    let mut batches = dataframe.execute_stream().await?;
    let (bytes_scanned, bytes_skipped) = pruning_metrics(&request);
    let mut pending: Option<RecordBatch> = None;

    while let Some(batch) = batches.next().await {
        let batch = batch?;
        if let Some(previous) = pending.replace(batch) {
            let result = make_result(
                &request,
                &previous,
                aggregated,
                false,
                bytes_scanned,
                bytes_skipped,
            )?;
            send_result(&sender, result).await?;
        }
    }
    let terminal = pending.unwrap_or_else(|| RecordBatch::new_empty(output_schema));
    let result = make_result(
        &request,
        &terminal,
        aggregated,
        true,
        bytes_scanned,
        bytes_skipped,
    )?;
    send_result(&sender, result).await?;
    Ok(())
}

fn make_result(
    request: &TaskRequest,
    batch: &RecordBatch,
    aggregated: bool,
    complete: bool,
    bytes_scanned: i64,
    bytes_skipped: i64,
) -> Result<PartialResult, Box<dyn std::error::Error + Send + Sync>> {
    Ok(PartialResult {
        task_id: request.task_id.clone(),
        is_aggregated: aggregated,
        column_names: batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect(),
        rows: Vec::new(),
        groups: if aggregated {
            aggregation_groups(batch, request)
        } else {
            Vec::new()
        },
        is_complete: complete,
        rows_scanned: request.partition_row_count,
        bytes_scanned,
        bytes_skipped,
        peak_memory_bytes: 0,
        arrow_ipc: if aggregated || batch.num_rows() == 0 {
            Vec::new()
        } else {
            batch_to_ipc(batch)?
        },
        sketch_state: Vec::new(),
        shuffle_paths: Vec::new(),
        bytes_written: 0,
        cpu_time_micros: 0,
        cache_hit: false,
        cache_level: String::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_probe_selection_uses_every_explicit_primary_partition() {
        let request = TaskRequest {
            partition_path: "fallback.parquet".to_owned(),
            primary_partition_paths: vec![
                "probe-0.parquet".to_owned(),
                "probe-1.parquet".to_owned(),
            ],
            ..Default::default()
        };
        assert_eq!(
            primary_object_keys(&request),
            vec!["probe-0.parquet".to_owned(), "probe-1.parquet".to_owned()]
        );
    }

    #[test]
    fn join_probe_selection_falls_back_to_the_assigned_partition() {
        let request = TaskRequest {
            partition_path: "assigned.parquet".to_owned(),
            ..Default::default()
        };
        assert_eq!(
            primary_object_keys(&request),
            vec!["assigned.parquet".to_owned()]
        );
    }
}
