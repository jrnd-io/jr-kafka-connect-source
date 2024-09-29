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

package io.jrnd.kafka.connect.connector.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StructHelper {

    private static final Logger LOG = LoggerFactory.getLogger(StructHelper.class);

    public static Struct convertJsonToStruct(Schema schema, String jsonString) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(jsonString);

        Struct struct = new Struct(schema);

        populateStruct(struct, schema, jsonNode);

        return struct;
    }

    public static void dumpSchema(Schema schema) {
        if (schema == null) {
            if (LOG.isDebugEnabled())
                LOG.debug("Schema is null");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Schema Name: ").append(schema.name()).append("\n");
        sb.append("Schema Type: ").append(schema.type()).append("\n");

        if (schema.isOptional()) {
            sb.append("Schema is Optional\n");
        } else {
            sb.append("Schema is Required\n");
        }

        switch (schema.type()) {
            case STRUCT:
                sb.append("Fields:\n");
                for (Field field : schema.fields()) {
                    sb.append("  Field Name: ").append(field.name()).append("\n");
                    sb.append("  Field Schema: ").append(field.schema().name())
                            .append(" (Type: ").append(field.schema().type()).append(")\n");
                }
                break;

            case ARRAY:
                sb.append("Array Type: ").append(schema.valueSchema().type()).append("\n");
                break;

            case MAP:
                sb.append("Map Key Type: ").append(schema.keySchema().type()).append("\n");
                sb.append("Map Value Type: ").append(schema.valueSchema().type()).append("\n");
                break;

            default:
                sb.append("Type Details: ").append(schema.type()).append("\n");
                break;
        }

        if (LOG.isDebugEnabled())
            LOG.debug("Schema --> {}", sb);
    }

    private static void populateStruct(Struct struct, Schema schema, JsonNode jsonNode) {
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            String fieldName = field.name();
            Schema fieldSchema = field.schema();
            JsonNode fieldValue = jsonNode.get(fieldName);

            if (fieldValue != null && !fieldValue.isNull()) {
                Object value = getValueFromJsonNode(fieldSchema, fieldValue);
                struct.put(fieldName, value);
            } else if (fieldSchema.isOptional()) {
                struct.put(fieldName, null);  // Optional fields can be set to null
            }
        }
    }

    private static Object getValueFromJsonNode(Schema schema, JsonNode jsonNode) {
        switch (schema.type()) {
            case STRING:
                return jsonNode.asText();
            case INT32:
                return jsonNode.asInt();
            case INT64:
                return jsonNode.asLong();
            case FLOAT32:
                return (float) jsonNode.asDouble();
            case FLOAT64:
                return jsonNode.asDouble();
            case BOOLEAN:
                return jsonNode.asBoolean();
            case STRUCT:
                Struct nestedStruct = new Struct(schema);
                populateStruct(nestedStruct, schema, jsonNode);
                return nestedStruct;
            case ARRAY:
                // Handle arrays (assumes homogeneous array elements)
                Schema elementSchema = schema.valueSchema();

                List results = new ArrayList();

                jsonNode.elements().forEachRemaining(element -> {
                    results.add(getValueFromJsonNode(elementSchema, element));
                });

                return results;

            case MAP:
                // Handle maps (Kafka Connect maps typically have STRING keys)
                return handleMap(schema, jsonNode);
            default:
                throw new IllegalArgumentException("Unsupported schema type: " + schema.type());
        }
    }

    private static Map<String, Object> handleMap(Schema schema, JsonNode jsonNode) {
        Map<String, Object> map = new java.util.HashMap<>();
        Schema valueSchema = schema.valueSchema();

        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String key = field.getKey();
            JsonNode valueNode = field.getValue();
            map.put(key, getValueFromJsonNode(valueSchema, valueNode));
        }

        return map;
    }

}