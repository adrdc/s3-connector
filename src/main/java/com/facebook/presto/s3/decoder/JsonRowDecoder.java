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
 * Note: This class file is from PrestoDb and contains an additional method. Specifically, the
 * decodeRow(data, offset, length, dataMap) method.
 * (commit hash e7b5e7c11031ec7d357131d20a9e67801b26a081)
 * https://github.com/prestodb/presto/blob/e7b5e7c11031ec7d357131d20a9e67801b26a081/presto-record-decoder/src/main/java/com/facebook/presto/decoder/json/JsonRowDecoder.java
 */
package com.facebook.presto.s3.decoder;

import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.json.JsonFieldDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * JSON specific row decoder.
 */
public class JsonRowDecoder
        implements RowDecoder {
    public static final String NAME = "json";

    private final ObjectMapper objectMapper;
    private final Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders;

    JsonRowDecoder(ObjectMapper objectMapper, Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders) {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.fieldDecoders = ImmutableMap.copyOf(fieldDecoders);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data) {
        JsonNode tree;
        try {
            tree = objectMapper.readTree(data);
        } catch (Exception e) {
            return Optional.empty();
        }

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();

        for (Map.Entry<DecoderColumnHandle, JsonFieldDecoder> entry : fieldDecoders.entrySet()) {
            DecoderColumnHandle columnHandle = entry.getKey();
            JsonFieldDecoder decoder = entry.getValue();
            JsonNode node = locateNode(tree, columnHandle);
            decodedRow.put(columnHandle, decoder.decode(node));
        }

        return Optional.of(decodedRow);
    }

    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data,
                                                                            int offset,
                                                                            int length) {
        JsonNode tree;
        try {
            tree = objectMapper.readTree(data, offset, length);
        } catch (Exception e) {
            return Optional.empty();
        }

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();

        for (Map.Entry<DecoderColumnHandle, JsonFieldDecoder> entry : fieldDecoders.entrySet()) {
            DecoderColumnHandle columnHandle = entry.getKey();
            JsonFieldDecoder decoder = entry.getValue();
            JsonNode node = locateNode(tree, columnHandle);
            decodedRow.put(columnHandle, decoder.decode(node));
        }

        return Optional.of(decodedRow);
    }

    private static JsonNode locateNode(JsonNode tree, DecoderColumnHandle columnHandle) {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());

        JsonNode currentNode = tree;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
            if (!currentNode.has(pathElement)) {
                return MissingNode.getInstance();
            }
            currentNode = currentNode.path(pathElement);
        }
        return currentNode;
    }
}
