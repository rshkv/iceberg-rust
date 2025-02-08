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

use std::collections::{HashMap, HashSet};
use std::string::ToString;
use std::sync::Arc;

use arrow_array::builder::{
    Int32Builder, Int64Builder, LargeBinaryBuilder, ListBuilder, MapBuilder, MapFieldNames,
    StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use async_stream::try_stream;
use futures::StreamExt;
use itertools::Itertools;

use crate::arrow::builder::AnyPrimitiveArrayBuilder;
use crate::arrow::{
    get_arrow_datum, schema_to_arrow_schema, type_to_arrow_type, DEFAULT_MAP_FIELD_NAME,
};
use crate::scan::ArrowRecordBatchStream;
use crate::spec::{
    join_schemas, DataFile, ManifestFile, NestedField, NestedFieldRef, PartitionField,
    PartitionSpec, PartitionSpecRef, PrimitiveType, Struct, TableMetadata, Transform, Type,
    MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME,
};
use crate::table::Table;
use crate::{Error, ErrorKind, Result};

/// Entries table containing the entries of the current snapshot's manifest files.
///
/// The table has one row for each manifest file entry in the current snapshot's manifest list file.
/// For reference, see the Java implementation of [`ManifestEntry`][1].
///
/// [1]: https://github.com/apache/iceberg/blob/apache-iceberg-1.7.1/core/src/main/java/org/apache/iceberg/ManifestEntry.java
pub struct EntriesTable<'a> {
    table: &'a Table,
}

impl<'a> EntriesTable<'a> {
    /// Create a new Entries table instance.
    pub fn new(table: &'a Table) -> Self {
        Self { table }
    }

    /// Get the schema for the manifest entries table.
    pub fn schema(&self) -> crate::spec::Schema {
        let schema = self.manifest_entry_schema();
        let readable_metric_schema = ReadableMetricsStructBuilder::readable_metrics_schema(
            self.table.metadata().current_schema(),
            &schema,
        );
        join_schemas(&schema, &readable_metric_schema).unwrap()
    }

    fn manifest_entry_schema(&self) -> crate::spec::Schema {
        let fields = vec![
            NestedField::required(0, "status", Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(1, "snapshot_id", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(3, "sequence_number", Type::Primitive(PrimitiveType::Long)),
            NestedField::optional(
                4,
                "file_sequence_number",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::required(
                2,
                "data_file",
                Type::Struct(
                    DataFileStructBuilder::schema(self.table.metadata())
                        .as_struct()
                        .clone(),
                ),
            ),
        ];
        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(Arc::new).collect_vec())
            .build()
            .unwrap()
    }

    /// Scan the manifest entries table.
    pub async fn scan(&self) -> Result<ArrowRecordBatchStream> {
        let current_snapshot = self.table.metadata().current_snapshot().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "Cannot scan entries for table without current snapshot",
            )
        })?;

        let manifest_list = current_snapshot
            .load_manifest_list(self.table.file_io(), self.table.metadata())
            .await?;

        // Copy to ensure that the stream can take ownership of these dependencies
        let schema = self.schema();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let table_metadata = self.table.metadata_ref();
        let file_io = Arc::new(self.table.file_io().clone());
        let readable_metrics_schema = schema
            .field_by_name("readable_metrics")
            .and_then(|field| field.field_type.clone().to_struct_type())
            .unwrap();
        let record_batch_options =
            Arc::new(RecordBatchOptions::new().with_match_field_names(false));

        Ok(try_stream! {
            for manifest_file in manifest_list.entries() {
                let mut status = Int32Builder::new();
                let mut snapshot_id = Int64Builder::new();
                let mut sequence_number = Int64Builder::new();
                let mut file_sequence_number = Int64Builder::new();
                let mut data_file = DataFileStructBuilder::try_new(&table_metadata);
                let mut readable_metrics =
                    ReadableMetricsStructBuilder::new(
                    table_metadata.current_schema(), &readable_metrics_schema);

                for manifest_entry in manifest_file.load_manifest(&file_io).await?.entries() {
                    status.append_value(manifest_entry.status() as i32);
                    snapshot_id.append_option(manifest_entry.snapshot_id());
                    sequence_number.append_option(manifest_entry.sequence_number());
                    file_sequence_number.append_option(manifest_entry.file_sequence_number());
                    data_file.append(manifest_file, manifest_entry.data_file())?;
                    readable_metrics.append(manifest_entry.data_file())?;
                }

                let batch = RecordBatch::try_new_with_options(arrow_schema.clone(), vec![
                    Arc::new(status.finish()),
                    Arc::new(snapshot_id.finish()),
                    Arc::new(sequence_number.finish()),
                    Arc::new(file_sequence_number.finish()),
                    Arc::new(data_file.finish()),
                    Arc::new(readable_metrics.finish()),
                ], &record_batch_options)?;

                yield batch;
            }
        }
        .boxed())
    }
}

/// Builds the struct describing data files listed in a table manifest.
///
/// For reference, see the Java implementation of [`DataFile`][1].
///
/// [1]: https://github.com/apache/iceberg/blob/apache-iceberg-1.7.1/api/src/main/java/org/apache/iceberg/DataFile.java
struct DataFileStructBuilder<'a> {
    // Reference to table metadata to retrieve partition specs based on partition spec ids
    table_metadata: &'a TableMetadata,
    // Below are the field builders of the "data_file" struct
    content: Int32Builder,
    file_path: StringBuilder,
    file_format: StringBuilder,
    partition: PartitionValuesStructBuilder,
    record_count: Int64Builder,
    file_size_in_bytes: Int64Builder,
    column_sizes: MapBuilder<Int32Builder, Int64Builder>,
    value_counts: MapBuilder<Int32Builder, Int64Builder>,
    null_value_counts: MapBuilder<Int32Builder, Int64Builder>,
    nan_value_counts: MapBuilder<Int32Builder, Int64Builder>,
    lower_bounds: MapBuilder<Int32Builder, LargeBinaryBuilder>,
    upper_bounds: MapBuilder<Int32Builder, LargeBinaryBuilder>,
    key_metadata: LargeBinaryBuilder,
    split_offsets: ListBuilder<Int64Builder>,
    equality_ids: ListBuilder<Int32Builder>,
    sort_order_ids: Int32Builder,
}

impl<'a> DataFileStructBuilder<'a> {
    fn try_new(table_metadata: &'a TableMetadata) -> Self {
        let map_field_names = Some(MapFieldNames {
            entry: DEFAULT_MAP_FIELD_NAME.to_string(),
            key: MAP_KEY_FIELD_NAME.to_string(),
            value: MAP_VALUE_FIELD_NAME.to_string(),
        });

        Self {
            table_metadata,
            content: Int32Builder::new(),
            file_path: StringBuilder::new(),
            file_format: StringBuilder::new(),
            partition: PartitionValuesStructBuilder::new(table_metadata),
            record_count: Int64Builder::new(),
            file_size_in_bytes: Int64Builder::new(),
            column_sizes: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                Int64Builder::new(),
            )
            .with_values_field(Arc::new(Field::new("value", DataType::Int64, false))),
            value_counts: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                Int64Builder::new(),
            )
            .with_values_field(Arc::new(Field::new("value", DataType::Int64, false))),
            null_value_counts: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                Int64Builder::new(),
            )
            .with_values_field(Arc::new(Field::new("value", DataType::Int64, false))),
            nan_value_counts: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                Int64Builder::new(),
            )
            .with_values_field(Arc::new(Field::new("value", DataType::Int64, false))),
            lower_bounds: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                LargeBinaryBuilder::new(),
            )
            .with_values_field(Arc::new(Field::new(
                "value",
                DataType::LargeBinary,
                false,
            ))),
            upper_bounds: MapBuilder::new(
                map_field_names.clone(),
                Int32Builder::new(),
                LargeBinaryBuilder::new(),
            )
            .with_values_field(Arc::new(Field::new(
                "value",
                DataType::LargeBinary,
                false,
            ))),
            key_metadata: LargeBinaryBuilder::new(),
            split_offsets: ListBuilder::new(Int64Builder::new()).with_field(Arc::new(Field::new(
                "element",
                DataType::Int64,
                false,
            ))),
            equality_ids: ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
                "element",
                DataType::Int32,
                false,
            ))),
            sort_order_ids: Int32Builder::new(),
        }
    }

    fn schema(table_metadata: &TableMetadata) -> crate::spec::Schema {
        let partition_type = PartitionValuesStructBuilder::partition_type(table_metadata);

        let fields = vec![
            NestedField::required(134, "content", Type::Primitive(PrimitiveType::Int)),
            NestedField::required(100, "file_path", Type::Primitive(PrimitiveType::String)),
            NestedField::required(101, "file_format", Type::Primitive(PrimitiveType::String)),
            NestedField::required(102, "partition", Type::Struct(partition_type)),
            NestedField::required(103, "record_count", Type::Primitive(PrimitiveType::Long)),
            NestedField::required(
                104,
                "file_size_in_bytes",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::required_map(108, "column_sizes")
                .key(117, Type::Primitive(PrimitiveType::Int))
                .value(118, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_map(109, "value_counts")
                .key(119, Type::Primitive(PrimitiveType::Int))
                .value(120, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_map(110, "null_value_counts")
                .key(121, Type::Primitive(PrimitiveType::Int))
                .value(122, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_map(137, "nan_value_counts")
                .key(138, Type::Primitive(PrimitiveType::Int))
                .value(139, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_map(125, "lower_bounds")
                .key(126, Type::Primitive(PrimitiveType::Int))
                .value(127, Type::Primitive(PrimitiveType::Binary), true)
                .build(),
            NestedField::required_map(128, "upper_bounds")
                .key(129, Type::Primitive(PrimitiveType::Int))
                .value(130, Type::Primitive(PrimitiveType::Binary), true)
                .build(),
            NestedField::optional(131, "key_metadata", Type::Primitive(PrimitiveType::Binary)),
            NestedField::required_list(132, "split_offsets")
                .element_field(133, Type::Primitive(PrimitiveType::Long), true)
                .build(),
            NestedField::required_list(135, "equality_ids")
                .element_field(136, Type::Primitive(PrimitiveType::Int), true)
                .build(),
            NestedField::optional(140, "sort_order_id", Type::Primitive(PrimitiveType::Int)),
        ];

        crate::spec::Schema::builder()
            .with_fields(fields.into_iter().map(Arc::new).collect_vec())
            .build()
            .unwrap()
    }

    fn append(&mut self, manifest_file: &ManifestFile, data_file: &DataFile) -> Result<()> {
        self.content.append_value(data_file.content as i32);
        self.file_path.append_value(data_file.file_path());
        self.file_format
            .append_value(data_file.file_format().to_string().to_uppercase());
        self.partition.append(
            self.partition_spec(manifest_file)?.clone().fields(),
            data_file.partition(),
        )?;
        self.record_count
            .append_value(data_file.record_count() as i64);
        self.file_size_in_bytes
            .append_value(data_file.file_size_in_bytes() as i64);

        // Sort keys to get matching order between rows
        for (k, v) in data_file.column_sizes.iter().sorted_by_key(|(k, _)| *k) {
            self.column_sizes.keys().append_value(*k);
            self.column_sizes.values().append_value(*v as i64);
        }
        self.column_sizes.append(true)?;

        for (k, v) in data_file.value_counts.iter().sorted_by_key(|(k, _)| *k) {
            self.value_counts.keys().append_value(*k);
            self.value_counts.values().append_value(*v as i64);
        }
        self.value_counts.append(true)?;

        for (k, v) in data_file
            .null_value_counts
            .iter()
            .sorted_by_key(|(k, _)| *k)
        {
            self.null_value_counts.keys().append_value(*k);
            self.null_value_counts.values().append_value(*v as i64);
        }
        self.null_value_counts.append(true)?;

        for (k, v) in data_file.nan_value_counts.iter().sorted_by_key(|(k, _)| *k) {
            self.nan_value_counts.keys().append_value(*k);
            self.nan_value_counts.values().append_value(*v as i64);
        }
        self.nan_value_counts.append(true)?;

        for (k, v) in data_file.lower_bounds.iter().sorted_by_key(|(k, _)| *k) {
            self.lower_bounds.keys().append_value(*k);
            self.lower_bounds.values().append_value(v.to_bytes()?);
        }
        self.lower_bounds.append(true)?;

        for (k, v) in data_file.upper_bounds.iter().sorted_by_key(|(k, _)| *k) {
            self.upper_bounds.keys().append_value(*k);
            self.upper_bounds.values().append_value(v.to_bytes()?);
        }
        self.upper_bounds.append(true)?;

        self.key_metadata.append_option(data_file.key_metadata());

        self.split_offsets
            .values()
            .append_slice(data_file.split_offsets());
        self.split_offsets.append(true);

        self.equality_ids
            .values()
            .append_slice(data_file.equality_ids());
        self.equality_ids.append(true);

        self.sort_order_ids.append_option(data_file.sort_order_id());
        Ok(())
    }

    fn partition_spec(&self, manifest_file: &ManifestFile) -> Result<&PartitionSpec> {
        self.table_metadata
            .partition_spec_by_id(manifest_file.partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Partition spec not found for manifest file",
                )
            })
            .map(|spec| spec.as_ref())
    }

    fn finish(&mut self) -> StructArray {
        let schema = schema_to_arrow_schema(&Self::schema(self.table_metadata)).unwrap();

        let inner_arrays: Vec<ArrayRef> = vec![
            Arc::new(self.content.finish()),
            Arc::new(self.file_path.finish()),
            Arc::new(self.file_format.finish()),
            Arc::new(self.partition.finish()),
            Arc::new(self.record_count.finish()),
            Arc::new(self.file_size_in_bytes.finish()),
            Arc::new(self.column_sizes.finish()),
            Arc::new(self.value_counts.finish()),
            Arc::new(self.null_value_counts.finish()),
            Arc::new(self.nan_value_counts.finish()),
            Arc::new(self.lower_bounds.finish()),
            Arc::new(self.upper_bounds.finish()),
            Arc::new(self.key_metadata.finish()),
            Arc::new(self.split_offsets.finish()),
            Arc::new(self.equality_ids.finish()),
            Arc::new(self.sort_order_ids.finish()),
        ];

        StructArray::from(
            schema
                .fields()
                .iter()
                .cloned()
                .zip_eq(inner_arrays)
                .collect_vec(),
        )
    }
}

/// Builds a readable metrics struct for a single column.
///
/// For reference, see [Java][1] and [Python][2] implementations.
///
/// [1]: https://github.com/apache/iceberg/blob/4a432839233f2343a9eae8255532f911f06358ef/core/src/main/java/org/apache/iceberg/MetricsUtil.java#L337
/// [2]: https://github.com/apache/iceberg-python/blob/a051584a3684392d2db6556449eb299145d47d15/pyiceberg/table/inspect.py#L101-L110
struct PerColumnReadableMetricsBuilder {
    data_table_field_id: i32,
    metadata_fields: Fields,
    column_size: Int64Builder,
    value_count: Int64Builder,
    null_value_count: Int64Builder,
    nan_value_count: Int64Builder,
    lower_bound: AnyPrimitiveArrayBuilder,
    upper_bound: AnyPrimitiveArrayBuilder,
}

impl PerColumnReadableMetricsBuilder {
    fn struct_type(field_id: &mut i32, data_type: &Type) -> crate::spec::StructType {
        let fields = vec![
            NestedField::optional(
                increment_and_get(field_id),
                "column_size",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                increment_and_get(field_id),
                "value_count",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                increment_and_get(field_id),
                "null_value_count",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                increment_and_get(field_id),
                "nan_value_count",
                Type::Primitive(PrimitiveType::Long),
            ),
            NestedField::optional(
                increment_and_get(field_id),
                "lower_bound",
                data_type.clone(),
            ),
            NestedField::optional(
                increment_and_get(field_id),
                "upper_bound",
                data_type.clone(),
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect_vec();
        crate::spec::StructType::new(fields)
    }

    fn new_for_field(
        data_table_field_id: i32,
        data_type: &DataType,
        metadata_fields: Fields,
    ) -> Self {
        Self {
            data_table_field_id,
            metadata_fields,
            column_size: Int64Builder::new(),
            value_count: Int64Builder::new(),
            null_value_count: Int64Builder::new(),
            nan_value_count: Int64Builder::new(),
            lower_bound: AnyPrimitiveArrayBuilder::new(data_type),
            upper_bound: AnyPrimitiveArrayBuilder::new(data_type),
        }
    }

    fn append(&mut self, data_file: &DataFile) -> Result<()> {
        self.column_size.append_option(
            data_file
                .column_sizes()
                .get(&self.data_table_field_id)
                .map(|&v| v as i64),
        );
        self.value_count.append_option(
            data_file
                .value_counts()
                .get(&self.data_table_field_id)
                .map(|&v| v as i64),
        );
        self.null_value_count.append_option(
            data_file
                .null_value_counts()
                .get(&self.data_table_field_id)
                .map(|&v| v as i64),
        );
        self.nan_value_count.append_option(
            data_file
                .nan_value_counts()
                .get(&self.data_table_field_id)
                .map(|&v| v as i64),
        );
        match data_file.lower_bounds().get(&self.data_table_field_id) {
            Some(datum) => self
                .lower_bound
                .append_datum(get_arrow_datum(datum)?.as_ref())?,
            None => self.lower_bound.append_null()?,
        }
        match data_file.upper_bounds().get(&self.data_table_field_id) {
            Some(datum) => self
                .upper_bound
                .append_datum(get_arrow_datum(datum)?.as_ref())?,
            None => self.upper_bound.append_null()?,
        }
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        let inner_arrays: Vec<ArrayRef> = vec![
            Arc::new(self.column_size.finish()),
            Arc::new(self.value_count.finish()),
            Arc::new(self.null_value_count.finish()),
            Arc::new(self.nan_value_count.finish()),
            Arc::new(self.lower_bound.finish()),
            Arc::new(self.upper_bound.finish()),
        ];

        StructArray::from(
            self.metadata_fields
                .into_iter()
                .cloned()
                .zip_eq(inner_arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )
    }
}

/// Build a [StructArray] with partition columns as fields and partition values as rows.
struct PartitionValuesStructBuilder {
    builders: Vec<AnyPrimitiveArrayBuilder>,
    partition_fields: Fields,
}

impl PartitionValuesStructBuilder {
    /// Construct a new builder from the combined partition columns of the table metadata.
    fn new(table_metadata: &TableMetadata) -> Self {
        let combined_struct_type = Self::partition_type(table_metadata);
        let DataType::Struct(partition_fields) =
            type_to_arrow_type(&Type::Struct(combined_struct_type)).unwrap()
        else {
            panic!("Converted Arrow type was not struct")
        };
        Self {
            builders: partition_fields
                .iter()
                .map(|field| AnyPrimitiveArrayBuilder::new(field.data_type()))
                .collect(),
            partition_fields,
        }
    }

    /// Builds a unified partition type considering all specs in the table.
    ///
    /// Based on Iceberg Java's [`Partitioning#partitionType`][1].
    ///
    /// [1]: https://github.com/apache/iceberg/blob/7e0cd3fa1e51d3c80f6c8cff23a03dca86f942fa/core/src/main/java/org/apache/iceberg/Partitioning.java#L240
    fn partition_type(table_metadata: &TableMetadata) -> crate::spec::StructType {
        Self::build_partition_projection_type(
            table_metadata.current_schema(),
            table_metadata.partition_specs_iter(),
            Self::all_fields_ids(table_metadata.partition_specs_iter()),
        )
    }

    /// Based on Iceberg Java's [`Partitioning#buildPartitionProjectionType`][1] with the difference
    /// that we pass along the [Schema] to map [PartitionField] to the current type.
    //
    /// [1]: https://github.com/apache/iceberg/blob/7e0cd3fa1e51d3c80f6c8cff23a03dca86f942fa/core/src/main/java/org/apache/iceberg/Partitioning.java#L255
    fn build_partition_projection_type<'a>(
        schema: &crate::spec::Schema,
        specs: impl Iterator<Item = &'a PartitionSpecRef>,
        projected_field_ids: HashSet<i32>,
    ) -> crate::spec::StructType {
        let mut field_map: HashMap<i32, PartitionField> = HashMap::new();
        let mut type_map: HashMap<i32, Type> = HashMap::new();
        let mut name_map: HashMap<i32, String> = HashMap::new();

        // Sort specs by ID in descending order to get latest field names
        let sorted_specs = specs
            .sorted_by_key(|spec| spec.spec_id())
            .rev()
            .collect_vec();

        for spec in sorted_specs {
            for field in spec.fields() {
                let field_id = field.field_id;

                if !projected_field_ids.contains(&field_id) {
                    continue;
                }

                let partition_type = spec.partition_type(schema).unwrap();
                let struct_field = partition_type.field_by_id(field_id).unwrap();
                let existing_field = field_map.get(&field_id);

                match existing_field {
                    None => {
                        field_map.insert(field_id, field.clone());
                        type_map.insert(field_id, struct_field.field_type.as_ref().clone());
                        name_map.insert(field_id, struct_field.name.clone());
                    }
                    Some(existing_field) => {
                        // verify the fields are compatible as they may conflict in v1 tables
                        if !Self::equivalent_ignoring_name(existing_field, field) {
                            panic!(
                                "Conflicting partition fields: ['{existing_field:?}', '{field:?}']",
                            );
                        }

                        // use the correct type for dropped partitions in v1 tables
                        if Self::is_void_transform(existing_field)
                            && !Self::is_void_transform(field)
                        {
                            field_map.insert(field_id, field.clone());
                            type_map.insert(field_id, struct_field.field_type.as_ref().clone());
                        }
                    }
                }
            }
        }

        let sorted_struct_fields = field_map
            .into_keys()
            .sorted()
            .map(|field_id| {
                NestedField::optional(field_id, &name_map[&field_id], type_map[&field_id].clone())
            })
            .map(Arc::new)
            .collect_vec();

        crate::spec::StructType::new(sorted_struct_fields)
    }

    fn is_void_transform(field: &PartitionField) -> bool {
        field.transform == Transform::Void
    }

    fn equivalent_ignoring_name(field: &PartitionField, another_field: &PartitionField) -> bool {
        field.field_id == another_field.field_id
            && field.source_id == another_field.source_id
            && Self::compatible_transforms(field.transform, another_field.transform)
    }

    fn compatible_transforms(t1: Transform, t2: Transform) -> bool {
        t1 == t2 || t1 == Transform::Void || t2 == Transform::Void
    }

    // collects IDs of all partition field used across specs
    fn all_fields_ids<'a>(specs: impl Iterator<Item = &'a PartitionSpecRef>) -> HashSet<i32> {
        specs
            .flat_map(|spec| spec.fields())
            .map(|partition| partition.field_id)
            .collect()
    }

    fn append(
        &mut self,
        partition_fields: &[PartitionField],
        partition_values: &Struct,
    ) -> Result<()> {
        for (field, value) in partition_fields.iter().zip_eq(partition_values.iter()) {
            let index = self.find_field(&field.name)?;

            match value {
                Some(literal) => self.builders[index].append_literal(literal)?,
                None => self.builders[index].append_null()?,
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map::<ArrayRef, _>(|builder| Arc::new(builder.finish()))
            .collect();
        StructArray::from(
            self.partition_fields
                .iter()
                .cloned()
                .zip_eq(arrays)
                .collect::<Vec<(FieldRef, ArrayRef)>>(),
        )
    }

    fn find_field(&self, name: &str) -> Result<usize> {
        match self.partition_fields.find(name) {
            Some((index, _)) => Ok(index),
            None => Err(Error::new(
                ErrorKind::Unexpected,
                format!("Field not found: {}", name),
            )),
        }
    }
}

struct ReadableMetricsStructBuilder {
    column_builders: Vec<PerColumnReadableMetricsBuilder>,
    column_fields: Fields,
}

impl ReadableMetricsStructBuilder {
    /// Calculates a dynamic schema for `readable_metrics` to add to metadata tables. The type
    /// will be a struct containing all primitive columns in the data table.
    ///
    /// We take the table's schema to get the set of fields in the table. We also take the manifest
    /// entry schema to get the highest field ID in the entries metadata table to know which field
    /// ID to begin with.
    fn readable_metrics_schema(
        data_table_schema: &crate::spec::Schema,
        manifest_entry_schema: &crate::spec::Schema,
    ) -> crate::spec::Schema {
        let mut next_id = manifest_entry_schema.highest_field_id();
        let mut per_column_readable_metrics_fields: Vec<NestedFieldRef> = Vec::new();

        for data_table_field in Self::sorted_primitive_fields(data_table_schema) {
            per_column_readable_metrics_fields.push(Arc::new(NestedField::required(
                increment_and_get(&mut next_id),
                &data_table_field.name,
                Type::Struct(PerColumnReadableMetricsBuilder::struct_type(
                    &mut next_id,
                    &data_table_field.field_type,
                )),
            )));
        }

        // let mut fields: Vec<NestedFieldRef> = Vec::new();
        // let id_to_name = data_table_schema.field_id_to_name_map();
        //
        // for id in id_to_name.keys() {
        //     let field = data_table_schema.field_by_id(*id).unwrap();
        //
        //     if field.field_type.is_primitive() {
        //         let col_name = id_to_name.get(id).unwrap();
        //
        //         fields.push(Arc::new(NestedField::optional(
        //             increment_and_get(&mut next_id),
        //             col_name,
        //             Type::Struct(PerColumnReadableMetricsBuilder::struct_type(
        //                 &mut next_id,
        //                 &field.field_type,
        //             )),
        //         )));
        //     }
        // }
        //
        // fields.sort_by_key(|field| field.name.clone());

        crate::spec::Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                increment_and_get(&mut next_id),
                "readable_metrics",
                Type::Struct(crate::spec::StructType::new(
                    per_column_readable_metrics_fields,
                )),
            ))])
            .build()
            .unwrap()
    }

    fn sorted_primitive_fields(data_table_schema: &crate::spec::Schema) -> Vec<NestedFieldRef> {
        let mut fields = data_table_schema
            .as_struct()
            .fields()
            .iter()
            .filter(|field| field.field_type.is_primitive())
            .cloned()
            .collect_vec();
        fields.sort_by_key(|field| field.name.clone());
        fields
    }

    fn new(
        data_table_schema: &crate::spec::Schema,
        readable_metrics_schema: &crate::spec::StructType,
    ) -> ReadableMetricsStructBuilder {
        let DataType::Struct(column_fields) =
            type_to_arrow_type(&Type::Struct(readable_metrics_schema.clone())).unwrap()
        else {
            panic!("Converted Arrow type was not struct")
        };
        let column_builders = readable_metrics_schema
            .fields()
            .iter()
            .zip_eq(Self::sorted_primitive_fields(data_table_schema))
            .map(|(readable_metrics_field, data_field)| {
                let DataType::Struct(fields) =
                    type_to_arrow_type(&readable_metrics_field.field_type).unwrap()
                else {
                    panic!("Readable metrics field was not a struct")
                };
                let arrow_type = type_to_arrow_type(&data_field.field_type).unwrap();
                PerColumnReadableMetricsBuilder::new_for_field(data_field.id, &arrow_type, fields)
            })
            .collect_vec();

        Self {
            column_fields,
            column_builders,
        }
    }

    fn append(&mut self, data_file: &DataFile) -> Result<()> {
        for column_builder in &mut self.column_builders {
            column_builder.append(data_file)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> StructArray {
        let arrays: Vec<ArrayRef> = self
            .column_builders
            .iter_mut()
            .map::<ArrayRef, _>(|builder| Arc::new(builder.finish()))
            .collect();

        let inner_arrays: Vec<(FieldRef, ArrayRef)> = self
            .column_fields
            .into_iter()
            .cloned()
            .zip_eq(arrays)
            .collect_vec();

        StructArray::from(inner_arrays)
    }
}

fn increment_and_get(value: &mut i32) -> i32 {
    *value += 1;
    *value
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use crate::inspect::metadata_table::tests::check_record_batches;
    use crate::scan::tests::TableTestFixture;

    #[tokio::test]
    async fn test_entries_table() {
        let mut fixture = TableTestFixture::new();
        fixture.setup_manifest_files().await;
        let table = fixture.table;
        let inspect = table.inspect();
        let entries_table = inspect.entries();

        let batch_stream = entries_table.scan().await.unwrap();

        check_record_batches(
            batch_stream,
            expect![[r#"
                Field { name: "status", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "0"} },
                Field { name: "snapshot_id", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "1"} },
                Field { name: "sequence_number", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "3"} },
                Field { name: "file_sequence_number", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "4"} },
                Field { name: "data_file", data_type: Struct([Field { name: "content", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "134"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "2"} },
                Field { name: "readable_metrics", data_type: Struct([Field { name: "a", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "136"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "137"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "138"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "139"} }, Field { name: "lower_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "140"} }, Field { name: "upper_bound", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "141"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "135"} }, Field { name: "binary", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "143"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "144"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "145"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "146"} }, Field { name: "lower_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "147"} }, Field { name: "upper_bound", data_type: LargeBinary, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "148"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "142"} }, Field { name: "bool", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "150"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "151"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "152"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "153"} }, Field { name: "lower_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "154"} }, Field { name: "upper_bound", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "155"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "149"} }, Field { name: "date", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "157"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "158"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "159"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "160"} }, Field { name: "lower_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "161"} }, Field { name: "upper_bound", data_type: Date32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "162"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "156"} }, Field { name: "dbl", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "164"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "165"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "166"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "167"} }, Field { name: "lower_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "168"} }, Field { name: "upper_bound", data_type: Float64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "169"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "163"} }, Field { name: "decimal", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "171"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "172"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "173"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "174"} }, Field { name: "lower_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "175"} }, Field { name: "upper_bound", data_type: Decimal128(3, 2), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "176"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "170"} }, Field { name: "float", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "178"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "179"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "180"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "181"} }, Field { name: "lower_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "182"} }, Field { name: "upper_bound", data_type: Float32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "183"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "177"} }, Field { name: "i32", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "185"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "186"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "187"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "188"} }, Field { name: "lower_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "189"} }, Field { name: "upper_bound", data_type: Int32, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "190"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "184"} }, Field { name: "i64", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "192"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "193"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "194"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "195"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "196"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "197"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "191"} }, Field { name: "timestamp", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "199"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "200"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "201"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "202"} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "203"} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "204"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "198"} }, Field { name: "timestampns", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "206"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "207"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "208"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "209"} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "210"} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, None), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "211"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "205"} }, Field { name: "timestamptz", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "213"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "214"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "215"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "216"} }, Field { name: "lower_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "217"} }, Field { name: "upper_bound", data_type: Timestamp(Microsecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "218"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "212"} }, Field { name: "timestamptzns", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "220"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "221"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "222"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "223"} }, Field { name: "lower_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "224"} }, Field { name: "upper_bound", data_type: Timestamp(Nanosecond, Some("+00:00")), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "225"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "219"} }, Field { name: "x", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "227"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "228"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "229"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "230"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "231"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "232"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "226"} }, Field { name: "y", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "234"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "235"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "236"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "237"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "238"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "239"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "233"} }, Field { name: "z", data_type: Struct([Field { name: "column_size", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "241"} }, Field { name: "value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "242"} }, Field { name: "null_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "243"} }, Field { name: "nan_value_count", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "244"} }, Field { name: "lower_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "245"} }, Field { name: "upper_bound", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "246"} }]), nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "240"} }]), nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {"PARQUET:field_id": "247"} }"#]],
            expect![[r#"
                +--------+---------------------+-----------------+----------------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | status | snapshot_id         | sequence_number | file_sequence_number | data_file    | readable_metrics                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
                +--------+---------------------+-----------------+----------------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | 1      | 3055729675574597004 | 1               | 1                    | {content: 0} | {a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: Apache, upper_bound: Iceberg}, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: false, upper_bound: true}, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01, upper_bound: 1970-01-01}, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100.0, upper_bound: 200.0}, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100.0, upper_bound: 200.0}, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100, upper_bound: 200}, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 100, upper_bound: 200}, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01T00:00:00, upper_bound: 1970-01-01T00:00:00}, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 1970-01-01T00:00:00Z, upper_bound: 1970-01-01T00:00:00Z}, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, x: {column_size: 1, value_count: 2, null_value_count: 3, nan_value_count: 4, lower_bound: 1, upper_bound: 1}, y: {column_size: 1, value_count: 2, null_value_count: 3, nan_value_count: 4, lower_bound: 2, upper_bound: 5}, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: 3, upper_bound: 4}} |
                | 2      | 3055729675574597004 | 0               | 0                    | {content: 0} | {a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, x: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, y: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }}                                                                                                                                                                       |
                | 0      | 3051729675574597004 | 0               | 0                    | {content: 0} | {a: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, binary: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, bool: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, date: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, dbl: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, decimal: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, float: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i32: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, i64: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamp: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestampns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptz: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, timestamptzns: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, x: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, y: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }, z: {column_size: , value_count: , null_value_count: , nan_value_count: , lower_bound: , upper_bound: }}                                                                                                                                                                       |
                +--------+---------------------+-----------------+----------------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"#]],
            &[],
            &["file_path"],
            None,
        )
            .await;
    }
}
