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

package io.jrnd.kafka.connect.format.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.jrnd.kafka.connect.connector.format.avro.AvroHelper;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class AvroHelperTest {

    @Test
    public void testCreateAvroSchemaFromJson_simpleJson() throws JsonProcessingException {
        String jsonString = "{ \"name\": \"John\", \"age\": 30, \"isEmployee\": true }";
        String recordName = "Person";

        Schema avroSchema = AvroHelper.createAvroSchemaFromJson(recordName, jsonString);

        assertEquals(recordName, avroSchema.getName());
        assertNotNull(avroSchema.getField("name"));
        assertEquals(Schema.Type.STRING, avroSchema.getField("name").schema().getType());

        assertNotNull(avroSchema.getField("age"));
        assertEquals(Schema.Type.INT, avroSchema.getField("age").schema().getType());

        assertNotNull(avroSchema.getField("isEmployee"));
        assertEquals(Schema.Type.BOOLEAN, avroSchema.getField("isEmployee").schema().getType());
    }

    @Test
    public void testCreateAvroSchemaFromJson_nestedJson() throws JsonProcessingException {
        String jsonString = "{ \"name\": \"John\", \"address\": { \"street\": \"Main St\", \"city\": \"Metropolis\" }, \"age\": 30 }";
        String recordName = "Person";

        Schema avroSchema = AvroHelper.createAvroSchemaFromJson(recordName, jsonString);

        assertNotNull(avroSchema.getField("address"));
        Schema addressSchema = avroSchema.getField("address").schema();

        assertEquals(Schema.Type.RECORD, addressSchema.getType());
        assertNotNull(addressSchema.getField("street"));
        assertNotNull(addressSchema.getField("city"));
    }

    @Test
    public void testConvertAvroToConnectSchema_basicTypes() {
        Schema avroSchemaString = Schema.create(Schema.Type.STRING);
        org.apache.kafka.connect.data.Schema connectSchemaString = AvroHelper.convertAvroToConnectSchema(avroSchemaString);
        assertEquals(org.apache.kafka.connect.data.Schema.Type.STRING, connectSchemaString.type());

        Schema avroSchemaInt = Schema.create(Schema.Type.INT);
        org.apache.kafka.connect.data.Schema connectSchemaInt = AvroHelper.convertAvroToConnectSchema(avroSchemaInt);
        assertEquals(org.apache.kafka.connect.data.Schema.Type.INT32, connectSchemaInt.type());

        Schema avroSchemaBoolean = Schema.create(Schema.Type.BOOLEAN);
        org.apache.kafka.connect.data.Schema connectSchemaBoolean = AvroHelper.convertAvroToConnectSchema(avroSchemaBoolean);
        assertEquals(org.apache.kafka.connect.data.Schema.Type.BOOLEAN, connectSchemaBoolean.type());
    }

    @Test
    public void testConvertAvroToConnectSchema_arrayType() {
        Schema avroArraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));

        org.apache.kafka.connect.data.Schema connectArraySchema = AvroHelper.convertAvroToConnectSchema(avroArraySchema);

        assertEquals(org.apache.kafka.connect.data.Schema.Type.ARRAY, connectArraySchema.type());
        assertEquals(org.apache.kafka.connect.data.Schema.Type.STRING, connectArraySchema.valueSchema().type());
    }

    @Test
    public void testConvertAvroToConnectSchema_mapType() {
        Schema avroMapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));

        org.apache.kafka.connect.data.Schema connectMapSchema = AvroHelper.convertAvroToConnectSchema(avroMapSchema);

        assertEquals(org.apache.kafka.connect.data.Schema.Type.MAP, connectMapSchema.type());
        assertEquals(org.apache.kafka.connect.data.Schema.Type.STRING, connectMapSchema.keySchema().type());
        assertEquals(org.apache.kafka.connect.data.Schema.Type.STRING, connectMapSchema.valueSchema().type());
    }

    @Test
    public void testConvertAvroToConnectSchema_nullableUnion() {
        Schema unionSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        org.apache.kafka.connect.data.Schema connectSchema = AvroHelper.convertAvroToConnectSchema(unionSchema);

        assertEquals(org.apache.kafka.connect.data.Schema.Type.STRING, connectSchema.type());
    }

    @Test
    public void testConvertAvroToConnectSchema_complexUnion() {
        Schema complexUnion = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING));
        org.apache.kafka.connect.data.Schema connectSchema = AvroHelper.convertAvroToConnectSchema(complexUnion);

        assertEquals(org.apache.kafka.connect.data.Schema.Type.INT32, connectSchema.type()); // Should pick the first non-null type
    }
}
