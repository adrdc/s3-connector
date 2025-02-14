/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.s3;

import io.airlift.log.Logger;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.Type;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.presto.s3.S3Const.*;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static com.facebook.presto.s3.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class S3Metadata
        implements ConnectorMetadata {
    private final String connectorId;
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3Metadata.class);
    private Map<SchemaTableName, S3Table> tableDescriptions;
    private final S3SchemaRegistryManager schemaRegistryManager;
    private final Supplier<Map<SchemaTableName, S3Table>> s3TableDescriptionSupplier;

    @Inject
    public S3Metadata(S3ConnectorId connectorId,
                      Supplier<Map<SchemaTableName, S3Table>> s3TableDescriptionSupplier,
                      S3ConnectorConfig s3ConnectorConfig) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.s3TableDescriptionSupplier = s3TableDescriptionSupplier;
        requireNonNull(s3TableDescriptionSupplier, "s3TableDescriptionSupplier is null");
        this.tableDescriptions = s3TableDescriptionSupplier.get();
        this.schemaRegistryManager = new S3SchemaRegistryManager(s3ConnectorConfig);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        this.tableDescriptions = s3TableDescriptionSupplier.get();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            builder.add(tableName.getSchemaName());
        }
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public S3TableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        S3Table table = tableDescriptions.get(tableName);
        if (table == null) {
            return null;
        }
        return new S3TableHandle(
                connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getObjectDataFormat(),
                table.getFieldDelimiter(),
                table.getRecordDelimiter(),
                table.getHasHeaderRow(),
                table.getTableBucketName(),
                table.getTableBucketPrefix(),
                table.getSources());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        S3TableHandle s3TableHandle = checkType(table, S3TableHandle.class, "table");
        checkArgument(s3TableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        SchemaTableName tableName = new SchemaTableName(s3TableHandle.getSchemaName(), s3TableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName) {
        S3Table table = tableDescriptions.get(tableName);
        if (table == null) {
            return null;
        } else if (table.getColumns().isEmpty() && tableName.getSchemaName().equals("s3_buckets")) {
            throw new TrinoException(NOT_SUPPORTED, "MetaData Search is not Enabled for this Bucket");
        } else {
            return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
        }
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull) {

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName entry : tableDescriptions.keySet()) {
            if (!entry.getTableName().equalsIgnoreCase(NO_TABLES)) {
                builder.add(entry);
            }
        }
        return builder.build();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal principal) {
        if (schemaRegistryManager.schemaExists(schemaName)) {
            throw new TrinoException(S3ErrorCode.S3_SCHEMA_ALREADY_EXISTS, format("Schema %s already exists", schemaName));
        }
        schemaRegistryManager.createGroup(schemaName, session.getUser());
        this.tableDescriptions = s3TableDescriptionSupplier.get();
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName) {
        if (!schemaRegistryManager.schemaExists(schemaName)) {
            // Idempotent - not an error
            log.debug("Drop schema for DB/group/schema " + schemaName + " does not exist");
            // Force a schema refresh
            this.tableDescriptions = s3TableDescriptionSupplier.get();
            return;
        }
        schemaRegistryManager.dropGroup(schemaName);
        this.tableDescriptions = s3TableDescriptionSupplier.get();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting) {
        if (schemaRegistryManager.tableSchemaExists(tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName())) {
            throw new TrinoException(S3ErrorCode.S3_TABLE_ALREADY_EXISTS,
                    format("Table %s in schema %s already exists", tableMetadata.getTable().getTableName(), tableMetadata.getTable().getSchemaName()));
        }
        schemaRegistryManager.createTable(tableMetadata);
        this.tableDescriptions = s3TableDescriptionSupplier.get();
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode) {
        if (schemaRegistryManager.tableSchemaExists(tableMetadata.getTable().getSchemaName(), tableMetadata.getTable().getTableName())) {
            throw new TrinoException(S3ErrorCode.S3_TABLE_ALREADY_EXISTS, format("Table %s in schema %s already exists",
                    tableMetadata.getTable().getTableName(), tableMetadata.getTable().getSchemaName()));
        }

        ImmutableList.Builder<S3Column> s3columns = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            s3columns.add(new S3Column(column.getName(), column.getType(), null));
        }

        return new S3OutputTableHandle(
                connectorId,
                tableMetadata.getTable(),
                tableMetadata.getProperties(),
                s3columns.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {
        S3OutputTableHandle handle = (S3OutputTableHandle) tableHandle;
        ImmutableList.Builder<ColumnMetadata> ctm = ImmutableList.builder();
        for (S3Column s3Column : handle.getColumns()) {
            ctm.add(new ColumnMetadata(s3Column.getName(), s3Column.getType()));
        }
        ConnectorTableMetadata tableMetadata =
                new ConnectorTableMetadata(handle.getSchemaTableName(),
                        ctm.build().asList(), handle.getProperties());

        schemaRegistryManager.createTable(tableMetadata);
        this.tableDescriptions = s3TableDescriptionSupplier.get();
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle) {
        S3TableHandle s3TableHandle = checkType(tableHandle, S3TableHandle.class, "tableHandle");
        checkArgument(s3TableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        if (!schemaRegistryManager.tableSchemaExists(s3TableHandle.getSchemaName(), s3TableHandle.getTableName())) {
            // Idempotent - not an error
            log.debug("Drop table for table" + s3TableHandle.getTableName()
                    + " in DB/group/schema " + s3TableHandle.getSchemaName() + " does not exist");
            // Force a schema refresh
            this.tableDescriptions = s3TableDescriptionSupplier.get();
            return;
        }
        schemaRegistryManager.dropTable(s3TableHandle);
        this.tableDescriptions = s3TableDescriptionSupplier.get();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode) {
        S3TableHandle s3TableHandle = checkType(tableHandle, S3TableHandle.class, "tableHandle");
        S3Table s3Table = tableDescriptions.get(s3TableHandle.toSchemaTableName());
        String tableName = s3TableHandle.getTableName();
        String schemaName = s3TableHandle.getSchemaName();
        String tableBucketName = s3Table.getTableBucketName();
        String tableBucketPrefix = s3Table.getTableBucketPrefix();
        String objectDataFormat = s3Table.getObjectDataFormat();
        if (!schemaRegistryManager.tableSchemaExists(schemaName, tableName)) {
            throw new TableNotFoundException(new SchemaTableName(schemaName, tableName));
        }
        checkArgument(s3TableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");
        log.debug("Begin Insert for table: " + s3TableHandle.getTableName() + " with output format " + objectDataFormat);
        List<S3ColumnHandle> s3Cols = (List) getColumnHandles(session, s3TableHandle).values();
        List<String> columnNames = s3Cols.stream().map(S3ColumnHandle::getName).collect(Collectors.toList());
        List<Type> columnTypes = s3Cols.stream().map(S3ColumnHandle::getType).collect(Collectors.toList());

        return new S3InsertTableHandle(
                connectorId,
                schemaName,
                tableName,
                columnNames,
                columnTypes,
                tableBucketName,
                tableBucketPrefix,
                s3Table.getHasHeaderRow(),
                s3Table.getRecordDelimiter(),
                s3Table.getFieldDelimiter(),
                objectDataFormat);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics) {
        this.tableDescriptions = s3TableDescriptionSupplier.get();
        return Optional.empty();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        final String formatHint = null;
        final boolean keyDecoder = true;
        final boolean hidden = false;
        final boolean internal = false;

        S3TableHandle s3TableHandle = checkType(tableHandle, S3TableHandle.class, "tableHandle");
        checkArgument(s3TableHandle.getConnectorId().equals(connectorId), "tableHandle is not for this connector");

        S3Table table = tableDescriptions.get(s3TableHandle.toSchemaTableName());
        if (table == null) {
            throw new TableNotFoundException(s3TableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (S3Column column : table.getColumns()) {
            columnHandles.put(column.getName(), new S3ColumnHandle(
                    connectorId,
                    index,
                    column.getName(),
                    column.getType(),
                    /* mapping - for delimited fmt like csv it is ordinal position.  else name. */
                    delimitedFormat(table) ? String.valueOf(index) : column.getName(),
                    column.getDataFormat(),
                    formatHint,
                    keyDecoder,
                    hidden,
                    internal));

            index++;
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        checkState(prefix.getSchema().isPresent());

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tableNames;

        if (prefix.getTable().isEmpty()) {
            tableNames = listTables(session, Optional.of(requireNonNull(prefix.getSchema().get(),
                    "schemaName is null")));
        } else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
        }
        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        checkType(tableHandle, S3TableHandle.class, "tableHandle");
        return checkType(columnHandle, S3ColumnHandle.class, "columnHandle").getColumnMetadata();
    }

    static boolean delimitedFormat(S3Table table) {
        return table.getObjectDataFormat().equals(CSV) ||
                table.getObjectDataFormat().equals(TEXT);
    }
}
