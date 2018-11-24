/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.kinesisanalytics.flink.connectors.serialization;

import com.amazonaws.services.kinesisanalytics.flink.connectors.exception.SerializationException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

public class JsonSerializationSchemaTest {

    private JsonSerializationSchema<TestSerializable> serializationSchema;

    @BeforeMethod
    public void init() {
        serializationSchema = new JsonSerializationSchema<>();
    }

    @Test
    public void testJsonSerializationSchemaHappyCase() {
        TestSerializable serializable = new TestSerializable(1, "Test description");
        byte[] serialized = serializationSchema.serialize(serializable);
        assertNotNull(serialized);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testJsonSerializationSchemaNullCase() {
        serializationSchema.serialize(null);
    }

    @Test(expectedExceptions = SerializationException.class,
        expectedExceptionsMessageRegExp = "Failed trying to serialize.*")
    public void testJsonSerializationSchemaInvalidSerializable() {
        JsonSerializationSchema<TestInvalidSerializable> serializationSchema =
            new JsonSerializationSchema<>();

        TestInvalidSerializable invalidSerializable = new TestInvalidSerializable("Unit", "Test");
        serializationSchema.serialize(invalidSerializable);
    }

    private static class TestSerializable {
        @JsonSerialize
        private final int id;
        @JsonSerialize
        private final String description;

        @JsonCreator
        public TestSerializable(final int id, final String desc) {
            this.id = id;
            this.description = desc;
        }
    }

    private static class TestInvalidSerializable {
        private final String firstName;
        private final String lastName;

        public TestInvalidSerializable(final String firstName, final String lastName) {
            this.firstName = firstName;
            this.lastName = lastName;
        }
    }
}
