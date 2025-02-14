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

import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonModule;
import io.airlift.log.Logger;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSinkProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class S3ConnectorFactory
        implements ConnectorFactory {
    private static final Logger log = Logger.get(com.facebook.presto.s3.S3ConnectorFactory.class);
    private final ClassLoader classLoader;

    private final Optional<Supplier<Map<SchemaTableName, S3Table>>> tableDescriptionSupplier;

    public S3ConnectorFactory(Optional<Supplier<Map<SchemaTableName, S3Table>>> tableDescriptionSupplier, ClassLoader classLoader) {

        this.classLoader = classLoader;
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    @Override
    public String getName() {
        return "s3";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");
        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new S3Module(catalogName),
                    binder -> {
                        binder.bind(S3ConnectorId.class).toInstance(new S3ConnectorId(catalogName));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        if (tableDescriptionSupplier.isPresent()) {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, S3Table>>>() {
                            }).toInstance(tableDescriptionSupplier.get());
                        } else {
                            binder.bind(new TypeLiteral<Supplier<Map<SchemaTableName, S3Table>>>() {
                            }).to(S3TableDescriptionSupplier.class).in(Scopes.SINGLETON);
                        }
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            S3Metadata s3Metadata = injector.getInstance(S3Metadata.class);
            S3SplitManager s3SplitManager = injector.getInstance(S3SplitManager.class);
            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            S3PageSourceProvider pageSourceProvider = injector.getInstance(S3PageSourceProvider.class);
            S3PageSinkProvider pageSinkProvider = injector.getInstance(S3PageSinkProvider.class);

            // Use classLoader because of some conflicts with jersey library versions
            return new S3Connector(lifeCycleManager,
                    new ClassLoaderSafeConnectorMetadata(s3Metadata, classLoader),
                    new ClassLoaderSafeConnectorSplitManager(s3SplitManager, classLoader),
                    new ClassLoaderSafeConnectorPageSourceProvider(pageSourceProvider, classLoader),
                    new ClassLoaderSafeConnectorPageSinkProvider(pageSinkProvider, classLoader));
        } catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
