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

package com.facebook.presto.s3.parquet;

import alluxio.shaded.client.com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.ChunkReader;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.plugin.hive.util.FSDataInputStreamTail;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.s3.S3ErrorCode.S3_FILESYSTEM_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class S3ParquetDataSource implements ParquetDataSource {
    private final FSDataInputStream inputStream;

    private final ParquetDataSourceId id;

    private long readTimeNanos;

    private long readBytes;

    private long estimatedSize;

    public S3ParquetDataSource(ParquetDataSourceId id, long estimatedSize, FSDataInputStream inputStream) {
        this.id = requireNonNull(id, "id is null");
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        this.estimatedSize = estimatedSize;
    }

    @Override
    public void close()
            throws IOException {
        inputStream.close();
    }

    @Override
    public ParquetDataSourceId getId() {
        return id;
    }

    @Override
    public long getReadBytes() {
        return readBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return readTimeNanos;
    }

    @Override
    public long getEstimatedSize() {
        return estimatedSize;
    }

    @Override
    public Slice readTail(int length) {
        try {
            long startTime = System.nanoTime();

            FSDataInputStreamTail tail =
                    FSDataInputStreamTail.readTail(getId().toString(), getEstimatedSize(), inputStream, length);
            Slice slice = tail.getTailSlice();

            readBytes += slice.length();
            readTimeNanos += System.nanoTime() - startTime;

            return slice;
        } catch (Exception e) {
            throw new TrinoException(S3_FILESYSTEM_ERROR, format("Error reading tail %s, length %s", getId(), length), e);
        }
    }

    @Override
    public Slice readFully(long position, int bufferLength) {
        try {
            long startTime = System.nanoTime();

            byte[] buffer = new byte[bufferLength];
            inputStream.readFully(position, buffer, 0, bufferLength);

            readBytes += bufferLength;
            readTimeNanos += System.nanoTime() - startTime;

            return Slices.wrappedBuffer(buffer);
        } catch (TrinoException e) {
            throw e;
        } catch (Exception e) {
            throw new TrinoException(S3_FILESYSTEM_ERROR, format("Error reading from %s at position %s", getId(), position), e);
        }
    }

    @Override
    public <K> ListMultimap<K, ChunkReader> planRead(ListMultimap<K, DiskRange> diskRanges) {
        return ImmutableListMultimap.of();
    }

    public static S3ParquetDataSource buildS3ParquetDataSource(FSDataInputStream inputStream, long estimatedSize, String bucketObjectName) {
        return new S3ParquetDataSource(new ParquetDataSourceId(bucketObjectName), estimatedSize, inputStream);
    }
}
