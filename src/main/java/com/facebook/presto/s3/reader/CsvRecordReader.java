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
package com.facebook.presto.s3.reader;

import io.airlift.log.Logger;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import com.facebook.presto.s3.BytesLineReader;
import com.facebook.presto.s3.CountingInputStream;
import com.facebook.presto.s3.S3ColumnHandle;
import com.facebook.presto.s3.S3ObjectRange;
import com.facebook.presto.s3.S3ReaderProps;
import com.facebook.presto.s3.S3TableLayoutHandle;
import com.facebook.presto.s3.decoder.CsvFieldValueProvider;
import com.facebook.presto.s3.decoder.CsvRecord;
import com.facebook.presto.s3.decoder.DateFieldValueProvider;
import io.trino.spi.type.Type;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.s3.S3Const.*;

public class CsvRecordReader
        implements RecordReader, Closeable {
    private static final Logger log = Logger.get(CsvRecordReader.class);

    private final S3ReaderProps readerProps;

    private final S3ObjectRange objectRange;

    private final S3TableLayoutHandle table;

    private final CsvRecord record;

    private BytesLineReader lineReader = null;

    private boolean haveRow = false;

    private final Map<DecoderColumnHandle, FieldValueProvider> row;

    private final Supplier<CountingInputStream> inputStreamSupplier;

    private CountingInputStream inputStream;

    public CsvRecordReader(List<S3ColumnHandle> columnHandles,
                           S3ObjectRange objectRange,
                           S3TableLayoutHandle table,
                           S3ReaderProps readerProps,
                           Supplier<CountingInputStream> inputStreamSupplier) {
        if (table.getTable().getFieldDelimiter().length() != 1) {
            throw new IllegalArgumentException(table.getTable().getFieldDelimiter());
        }

        this.readerProps = readerProps;
        this.objectRange = objectRange;
        this.table = table;
        this.inputStreamSupplier = inputStreamSupplier;

        this.record = new CsvRecord(table.getTable().getFieldDelimiter().charAt(0));

        // create col->value provider objects once as all the same underlying objects are used (i.e. record)
        this.row = new HashMap<>();
        for (S3ColumnHandle columnHandle : columnHandles) {
            this.row.put(columnHandle, getFieldValueProvider(columnHandle));
        }
    }

    private FieldValueProvider getFieldValueProvider(S3ColumnHandle columnHandle) {
        return isDateOrTimeType(columnHandle.getType())
                ? new DateFieldValueProvider(record, columnHandle)
                : new CsvFieldValueProvider(record, columnHandle);
    }

    private boolean isDateOrTimeType(Type type) {
        return type == DATE || type == TIME || type == TIMESTAMP;
    }

    private void init() {
        long end = objectRange.getCompressionType() != null
                ? Long.MAX_VALUE
                : objectRange.getOffset() + objectRange.getLength();

        long start = objectRange.getOffset();
        if (readerProps.getS3SelectEnabled() && objectRange.getSplit()) {
            // if using scan range with s3 select there is no seeking/spillover here
            start = 0;
            end = Long.MAX_VALUE;
        }

        inputStream = inputStreamSupplier.get();

        lineReader = new BytesLineReader(
                inputStream,
                readerProps.getBufferSizeBytes(),
                start, end);

        if (!readerProps.getS3SelectEnabled() &&
                objectRange.getOffset() == 0 &&
                table.getTable().getHasHeaderRow().equals(LC_TRUE)) {
            // eat the header
            lineReader.read(record.value);
        }
    }

    @Override
    public long getTotalBytes() {
        return inputStream == null
                ? 0
                : inputStream.getTotalBytes();
    }

    @Override
    public boolean hasNext() {
        if (haveRow) {
            return true;
        }
        return advance();
    }

    private boolean advance() {
        if (lineReader == null) {
            init();
        }

        record.len = lineReader.read(record.value);
        record.decoded = false;
        // 0 is empty row.  -1 is EOF.
        haveRow = record.len >= 0;
        return haveRow;
    }

    @Override
    public Map<DecoderColumnHandle, FieldValueProvider> next() {
        if (!haveRow) {
            if (!advance()) {
                return null;
            }
        }
        haveRow = false;
        return row;
    }

    @Override
    public void close() {
        if (lineReader != null) {
            lineReader.close();
        }
    }
}
