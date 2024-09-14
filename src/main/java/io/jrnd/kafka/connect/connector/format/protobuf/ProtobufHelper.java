package io.jrnd.kafka.connect.connector.format.protobuf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ProtobufHelper {

    private static final Map<DescriptorProtos.FieldDescriptorProto.Type, Schema> PROTOBUF_TO_KAFKA_CONNECT_TYPE_MAP = new HashMap<>();

    static {
        PROTOBUF_TO_KAFKA_CONNECT_TYPE_MAP.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, Schema.STRING_SCHEMA);
        PROTOBUF_TO_KAFKA_CONNECT_TYPE_MAP.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32, Schema.INT32_SCHEMA);
        PROTOBUF_TO_KAFKA_CONNECT_TYPE_MAP.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64, Schema.INT64_SCHEMA);
        PROTOBUF_TO_KAFKA_CONNECT_TYPE_MAP.put(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL, Schema.BOOLEAN_SCHEMA);
    }

    public static Schema createProtobufSchemaFromJson(String messageName, String jsonString) throws Exception {
        DescriptorProtos.DescriptorProto proto = createProtobufSchema(messageName, jsonString);
        return convertToKafkaConnectSchema(proto);
    }

    private static DescriptorProtos.DescriptorProto createProtobufSchema(String messageName, String jsonString) throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(jsonString);

        DescriptorProtos.DescriptorProto.Builder messageDescriptorBuilder = DescriptorProtos.DescriptorProto.newBuilder()
                .setName(messageName);

        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        int fieldNumber = 1;

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            String fieldName = entry.getKey();
            JsonNode fieldValue = entry.getValue();

            DescriptorProtos.FieldDescriptorProto.Type protoFieldType;
            if (fieldValue.isTextual()) {
                protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
            } else if (fieldValue.isInt()) {
                protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
            } else if (fieldValue.isLong()) {
                protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64;
            } else if (fieldValue.isBoolean()) {
                protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL;
            } else if (fieldValue.isObject()) {
                DescriptorProtos.DescriptorProto nestedMessage = createProtobufSchema(fieldName, fieldValue.toString());
                protoFieldType = DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;
                messageDescriptorBuilder.addNestedType(nestedMessage);
            } else {
                continue;
            }

            DescriptorProtos.FieldDescriptorProto fieldDescriptorProto = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName(fieldName)
                    .setNumber(fieldNumber++)
                    .setType(protoFieldType)
                    .build();

            messageDescriptorBuilder.addField(fieldDescriptorProto);
        }

        return messageDescriptorBuilder.build();
    }

    private static Schema convertToKafkaConnectSchema(DescriptorProtos.DescriptorProto descriptorProto) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(descriptorProto.getName());

        for (DescriptorProtos.FieldDescriptorProto field : descriptorProto.getFieldList()) {
            String fieldName = field.getName();
            DescriptorProtos.FieldDescriptorProto.Type fieldType = field.getType();

            if (fieldType == DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE) {
                DescriptorProtos.DescriptorProto nestedMessageDescriptor = findNestedMessage(descriptorProto, field);
                if (nestedMessageDescriptor != null) {
                    Schema nestedSchema = convertToKafkaConnectSchema(nestedMessageDescriptor);
                    schemaBuilder.field(fieldName, nestedSchema);
                }
            } else {
                Schema kafkaFieldSchema = PROTOBUF_TO_KAFKA_CONNECT_TYPE_MAP.get(fieldType);
                if (kafkaFieldSchema != null) {
                    schemaBuilder.field(fieldName, kafkaFieldSchema);
                }
            }
        }

        return schemaBuilder.build();
    }


    private static DescriptorProtos.DescriptorProto findNestedMessage(DescriptorProtos.DescriptorProto descriptorProto, DescriptorProtos.FieldDescriptorProto field) {
        for (DescriptorProtos.DescriptorProto nestedType : descriptorProto.getNestedTypeList()) {
            if (nestedType.getName().equals(field.getTypeName().substring(field.getTypeName().lastIndexOf(".") + 1))) {
                return nestedType;
            }
        }
        return null;
    }

}
