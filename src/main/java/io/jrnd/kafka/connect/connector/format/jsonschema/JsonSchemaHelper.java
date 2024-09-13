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

package io.jrnd.kafka.connect.connector.format.jsonschema;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public class JsonSchemaHelper {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static Schema createJsonSchemaFromJson(String jsonDocument) throws IllegalArgumentException, IOException {

        JsonNode properties = addProperties(OBJECT_MAPPER.readTree(jsonDocument));
        ObjectNode schema = OBJECT_MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.set("properties", properties);

        String jsonSchemaAsString = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(schema);
        JsonNode jsonSchema = OBJECT_MAPPER.readTree(jsonSchemaAsString);
        return convertJsonSchemaToConnectSchema(jsonSchema);
    }

    private static ObjectNode addProperties(JsonNode jsonData) throws IOException {
        ObjectNode propObject = OBJECT_MAPPER.createObjectNode();

        jsonData.fields().forEachRemaining(entry -> {
            String fieldName = entry.getKey();
            JsonNode fieldValue = entry.getValue();
            JsonNodeType fieldType = fieldValue.getNodeType();

            ObjectNode property;
            try {
                property = processJsonField(fieldValue, fieldType, fieldName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (!property.isEmpty()) {
                propObject.set(fieldName, property);
            }
        });

        return propObject;
    }

    private static ObjectNode processJsonField(JsonNode fieldValue, JsonNodeType fieldType, String fieldName)
            throws IOException {
        ObjectNode property = OBJECT_MAPPER.createObjectNode();

        switch (fieldType) {
            case ARRAY:
                property.put("type", "array");

                if (fieldValue.isEmpty())
                    break;

                JsonNodeType typeOfArrayElements = fieldValue.get(0).getNodeType();
                if (typeOfArrayElements.equals(JsonNodeType.OBJECT))
                    property.set("items", addProperties(fieldValue.get(0)));
                else
                    property.set("items", processJsonField(fieldValue.get(0), typeOfArrayElements, fieldName));
                break;
            case BOOLEAN:
                property.put("type", "boolean");
                break;
            case NUMBER:
                property.put("type", "number");
                break;
            case OBJECT:
                property.put("type", "object");
                property.set("properties", addProperties(fieldValue));
                break;
            case STRING:
                property.put("type", "string");
                break;
            default:
                break;
        }
        return property;
    }

    private static Schema convertJsonSchemaToConnectSchema(JsonNode jsonSchema) {
        String type = jsonSchema.get("type").asText();

        switch (type) {
            case "string":
                return Schema.STRING_SCHEMA;
            case "number":
                if (jsonSchema.has("format") && "float".equals(jsonSchema.get("format").asText())) {
                    return Schema.FLOAT32_SCHEMA;
                } else {
                    return Schema.FLOAT64_SCHEMA;
                }
            case "integer":
                if (jsonSchema.has("format") && "int32".equals(jsonSchema.get("format").asText())) {
                    return Schema.INT32_SCHEMA;
                } else {
                    return Schema.INT64_SCHEMA;
                }
            case "boolean":
                return Schema.BOOLEAN_SCHEMA;
            case "object":
                SchemaBuilder structBuilder = SchemaBuilder.struct();
                JsonNode properties = jsonSchema.get("properties");
                if (properties != null) {
                    properties.fields().forEachRemaining(entry -> {
                        String fieldName = entry.getKey();
                        JsonNode fieldSchema = entry.getValue();
                        Schema fieldKafkaSchema = convertJsonSchemaToConnectSchema(fieldSchema);
                        structBuilder.field(fieldName, fieldKafkaSchema);
                    });
                }
                return structBuilder.build();
            case "array":
                JsonNode items = jsonSchema.get("items");
                if (items != null) {
                    Schema elementSchema = convertJsonSchemaToConnectSchema(items);
                    return SchemaBuilder.array(elementSchema).build();
                } else {
                    throw new IllegalArgumentException("Array schema must have 'items' definition");
                }
            case "null":
                return Schema.OPTIONAL_STRING_SCHEMA;
            default:
                throw new IllegalArgumentException("Unsupported JSON Schema type: " + type);
        }
    }
}