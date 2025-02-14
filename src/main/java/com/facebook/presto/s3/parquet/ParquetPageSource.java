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
 *
 * Note: This file is from PrestoDb. Specifically is from the Hive Connector
 * https://github.com/prestodb/presto/blob/84ade36f51c10c449f62aa840426fa798d4c078d/presto-hive/src/main/java/com/facebook/presto/hive/parquet/ParquetPageSource.java
 */
package com.facebook.presto.s3.parquet;

import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import com.google.common.collect.ImmutableList;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.s3.S3ErrorCode.S3_BAD_DATA;
import static com.facebook.presto.s3.S3ErrorCode.S3_CURSOR_ERROR;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ParquetPageSource implements ConnectorPageSource {
    private final ParquetReader parquetReader;
    private final List<Type> types;
    private final List<Optional<Field>> fields;

    private int batchId;

    private long completedPositions;

    private boolean closed;

    public ParquetPageSource(
            ParquetReader parquetReader,
            List<Type> types,
            List<Optional<Field>> fields) {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.fields = ImmutableList.copyOf(requireNonNull(fields, "fields is null"));
    }

    @Override
    public long getCompletedBytes() {
        return parquetReader.getDataSource().getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions() {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos() {
        return parquetReader.getDataSource().getReadTimeNanos();
    }

    @Override
    public boolean isFinished() {
        return closed;
    }

    @Override
    public Page getNextPage() {
        try {
            batchId++;
            int batchSize = parquetReader.nextBatch();

            if (closed || batchSize <= 0) {
                close();
                return null;
            }

            completedPositions += batchSize;

            Block[] blocks = new Block[fields.size()];
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                Optional<Field> field = fields.get(fieldId);
                if (field.isPresent()) {
                    blocks[fieldId] = new LazyBlock(batchSize, new ParquetBlockLoader(field.get()));
                } else {
                    blocks[fieldId] = RunLengthEncodedBlock.create(types.get(fieldId), null, batchSize);
                }
            }
            return new Page(batchSize, blocks);
        } catch (TrinoException e) {
            closeWithSuppression(e);
            throw e;
        } catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new TrinoException(S3_CURSOR_ERROR, e);
        }
    }

    @Override
    public long getMemoryUsage() {
        return parquetReader.getMemoryContext().getBytes();
    }

    private void closeWithSuppression(Throwable throwable) {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        } catch (RuntimeException e) {
            // Self-suppression not permitted
            if (e != throwable) {
                throwable.addSuppressed(e);
            }
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        try {
            parquetReader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public CompletableFuture<?> isBlocked() {
        return ConnectorPageSource.super.isBlocked();
    }

    @Override
    public Metrics getMetrics() {
        return ConnectorPageSource.super.getMetrics();
    }

    private final class ParquetBlockLoader
            implements LazyBlockLoader {
        private final int expectedBatchId = batchId;
        private final Field field;

        private boolean loaded;

        public ParquetBlockLoader(Field field) {
            this.field = requireNonNull(field, "field is null");
        }

        @Override
        public Block load() {
            checkState(!loaded);
            checkState(batchId == expectedBatchId);

            Block block;
            try {
                block = parquetReader.readBlock(field);
            } catch (ParquetCorruptionException e) {
                throw new TrinoException(S3_BAD_DATA, e);
            } catch (IOException e) {
                throw new TrinoException(S3_CURSOR_ERROR, e);
            }

            loaded = true;
            return block;
        }
    }
}
