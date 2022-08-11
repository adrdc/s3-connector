package com.facebook.presto.s3.parquet;

import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.reader.MetadataReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;

public class ParquetMetadataSource {
    ParquetMetadata getParquetMetadata(ParquetDataSource parquetDataSource)
            throws IOException {
        return MetadataReader.readFooter(parquetDataSource);
    }
}
