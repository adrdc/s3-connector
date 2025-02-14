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

import com.facebook.presto.s3.parquet.ParquetMetadataSource;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.decoder.DispatchingRowDecoderFactory;
import io.trino.decoder.RowDecoderFactory;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.decoder.csv.CsvRowDecoder;
import io.trino.decoder.csv.CsvRowDecoderFactory;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.decoder.dummy.DummyRowDecoderFactory;
import io.trino.decoder.raw.RawRowDecoder;
import io.trino.decoder.raw.RawRowDecoderFactory;
import com.facebook.presto.s3.decoder.JsonRowDecoder;
import com.facebook.presto.s3.decoder.JsonRowDecoderFactory;
import com.facebook.presto.s3.parquet.ParquetPageSourceFactory;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.testing.TestingMBeanServer;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.management.MBeanServer;

import java.util.HashSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static java.util.Objects.requireNonNull;

public class S3Module
        extends AbstractConfigurationAwareModule {
    private final String connectorId;

    public S3Module(String connectorId) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    @Override
    public void setup(Binder binder) {
        binder.bind(S3ConnectorId.class).toInstance(new S3ConnectorId(connectorId));
        binder.bind(S3Metadata.class).in(SINGLETON);
        binder.bind(S3SplitManager.class).in(SINGLETON);
        binder.bind(MBeanServer.class).toInstance(new TestingMBeanServer());
        binder.bind(MBeanExporter.class).in(SINGLETON);
        binder.bind(S3AccessObject.class).in(SINGLETON);
        binder.bind(S3PageSourceProvider.class).in(SINGLETON);
        binder.bind(S3PageSinkProvider.class).in(SINGLETON);
        binder.bind(S3SchemaRegistryManager.class).in(SINGLETON);
        Multibinder<S3BatchPageSourceFactory> pageSourceFactoryBinder = newSetBinder(binder, S3BatchPageSourceFactory.class);
        pageSourceFactoryBinder.addBinding().to(ParquetPageSourceFactory.class).in(SINGLETON);
        configBinder(binder).bindConfig(S3ConnectorConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(S3Table.class);

        // We are creating our own DecoderModule so we can use our custom decoders when necessary.
        // For this case as an example, we are using our JsonRowDecoder and the JsonRowDecoderFactory (not from the Presto code itself).
        // The JsonRowDecoder and JsonRowDecoderFactory are both from our source code com.facebook.presto.s3.decoder
        binder.install(binder1 -> {
            MapBinder<String, RowDecoderFactory> decoderFactoriesByName = MapBinder.newMapBinder(binder1, String.class, RowDecoderFactory.class);
            decoderFactoriesByName.addBinding(DummyRowDecoder.NAME).to(DummyRowDecoderFactory.class).in(SINGLETON);
            decoderFactoriesByName.addBinding(CsvRowDecoder.NAME).to(CsvRowDecoderFactory.class).in(SINGLETON);
            decoderFactoriesByName.addBinding(RawRowDecoder.NAME).to(RawRowDecoderFactory.class).in(SINGLETON);
            decoderFactoriesByName.addBinding(AvroRowDecoderFactory.NAME).to(AvroRowDecoderFactory.class).in(SINGLETON);
            decoderFactoriesByName.addBinding(JsonRowDecoder.NAME).to(JsonRowDecoderFactory.class).in(SINGLETON);
            binder1.bind(DispatchingRowDecoderFactory.class).in(SINGLETON);
        });
    }

    @Singleton
    @Provides
    public ParquetMetadataSource createParquetMetadataSource() {
        return new ParquetMetadataSource();
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type> {
        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager) {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context) {
            Type type = typeManager.getType(parseTypeSignature(value, new HashSet<>()));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
