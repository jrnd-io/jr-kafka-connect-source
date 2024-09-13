// Copyright © 2024 JR team
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package io.jrnd.kafka.connect.connector.format.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AvroHelper {

    public static Schema createAvroSchemaFromJson(String recordName, String jsonString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(jsonString);

        return buildAvroSchema(recordName, jsonNode);
    }

    public static org.apache.kafka.connect.data.Schema convertAvroToConnectSchema(Schema avroSchema) {
        switch (avroSchema.getType()) {
            case STRING:
                return org.apache.kafka.connect.data.SchemaBuilder.string().build();
            case INT:
                return org.apache.kafka.connect.data.SchemaBuilder.int32().build();
            case LONG:
                return org.apache.kafka.connect.data.SchemaBuilder.int64().build();
            case FLOAT:
                return org.apache.kafka.connect.data.SchemaBuilder.float32().build();
            case DOUBLE:
                return org.apache.kafka.connect.data.SchemaBuilder.float64().build();
            case BOOLEAN:
                return org.apache.kafka.connect.data.SchemaBuilder.bool().build();
            case BYTES:
                return org.apache.kafka.connect.data.SchemaBuilder.bytes().build();
            case ARRAY:
                org.apache.kafka.connect.data.Schema elementSchema = convertAvroToConnectSchema(avroSchema.getElementType());
                return org.apache.kafka.connect.data.SchemaBuilder.array(elementSchema).build();
            case MAP:
                org.apache.kafka.connect.data.Schema valueSchema = convertAvroToConnectSchema(avroSchema.getValueType());
                return org.apache.kafka.connect.data.SchemaBuilder.map(org.apache.kafka.connect.data.Schema.STRING_SCHEMA, valueSchema).build();
            case RECORD:
                return convertRecord(avroSchema);
            case ENUM:
                return org.apache.kafka.connect.data.SchemaBuilder.string().build(); // Kafka Connect doesn't have native ENUM support, so use string.
            case UNION:
                return handleUnion(avroSchema);
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + avroSchema.getType());
        }
    }

    private static org.apache.kafka.connect.data.Schema convertRecord(Schema avroSchema) {
        org.apache.kafka.connect.data.SchemaBuilder structBuilder = org.apache.kafka.connect.data.SchemaBuilder.struct().name(avroSchema.getName());
        for (Schema.Field field : avroSchema.getFields()) {
            org.apache.kafka.connect.data.Schema fieldSchema = convertAvroToConnectSchema(field.schema());
            structBuilder.field(field.name(), fieldSchema);
        }
        return structBuilder.build();
    }

    private static org.apache.kafka.connect.data.Schema handleUnion(Schema unionSchema) {
        List<Schema> types = unionSchema.getTypes();
        if (types.size() == 2 && types.contains(Schema.create(Schema.Type.NULL))) {
            // Handle nullable types (e.g., ["null", "string"])
            Schema nonNullSchema = types.get(0).getType() == Schema.Type.NULL ? types.get(1) : types.get(0);
            return convertAvroToConnectSchema(nonNullSchema);
        } else {
            // If it's not a nullable type, pick the first non-null type or handle complex cases
            for (Schema schema : types) {
                if (schema.getType() != Schema.Type.NULL) {
                    return convertAvroToConnectSchema(schema);
                }
            }
            throw new IllegalArgumentException("Unsupported union schema: " + unionSchema);
        }
    }

    private static Schema buildAvroSchema(String recordName, JsonNode jsonNode) {
        SchemaBuilder.RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(recordName);
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = recordBuilder.fields();

        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();

            if (fieldValue.isTextual()) {
                fieldAssembler.name(fieldName).type().stringType().noDefault();
            } else if (fieldValue.isInt()) {
                fieldAssembler.name(fieldName).type().intType().noDefault();
            } else if (fieldValue.isLong()) {
                fieldAssembler.name(fieldName).type().longType().noDefault();
            } else if (fieldValue.isBoolean()) {
                fieldAssembler.name(fieldName).type().booleanType().noDefault();
            } else if (fieldValue.isDouble()) {
                fieldAssembler.name(fieldName).type().doubleType().noDefault();
            } else if (fieldValue.isObject()) {
                Schema nestedSchema = buildAvroSchema(fieldName, fieldValue);
                fieldAssembler.name(fieldName).type(nestedSchema).noDefault();
            } else if (fieldValue.isArray()) {
                if (fieldValue.size() > 0) {
                    JsonNode firstElement = fieldValue.get(0);
                    Schema elementType = getSchemaFromJsonNode(firstElement, fieldName);
                    fieldAssembler.name(fieldName).type().array().items(elementType).noDefault();
                } else {
                    fieldAssembler.name(fieldName).type().array().items().stringType().noDefault();  // Default empty arrays to strings
                }
            } else {
                fieldAssembler.name(fieldName).type().nullable().stringType().noDefault();
            }
        }
        return fieldAssembler.endRecord();
    }

    private static Schema getSchemaFromJsonNode(JsonNode node, String fieldName) {
        if (node.isTextual()) {
            return Schema.create(Schema.Type.STRING);
        } else if (node.isInt()) {
            return Schema.create(Schema.Type.INT);
        } else if (node.isLong()) {
            return Schema.create(Schema.Type.LONG);
        } else if (node.isBoolean()) {
            return Schema.create(Schema.Type.BOOLEAN);
        } else if (node.isDouble()) {
            return Schema.create(Schema.Type.DOUBLE);
        } else if (node.isObject()) {
            return buildAvroSchema(fieldName, node);
        } else {
            return Schema.create(Schema.Type.STRING);
        }
    }
}

