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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class S3Column {
    private final String name;
    private final Type type;
    private final String dataFormat;

    @JsonCreator
    public S3Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("dataFormat") String dataFormat) {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.dataFormat = dataFormat;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public String getDataFormat() {
        return dataFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, dataFormat);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        com.facebook.presto.s3.S3Column other = (com.facebook.presto.s3.S3Column) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.dataFormat, other.dataFormat);
    }

    @Override
    public String toString() {
        return name + ":" + type + " (" + dataFormat + ")";
    }
}
