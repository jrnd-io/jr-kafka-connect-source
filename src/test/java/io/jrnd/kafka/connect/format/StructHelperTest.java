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

package io.jrnd.kafka.connect.format;

import io.jrnd.kafka.connect.connector.format.StructHelper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class StructHelperTest {

    @Test
    void testConvertJsonToStruct() throws IOException {
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("isEmployed", Schema.BOOLEAN_SCHEMA)
                .build();

        String jsonString = "{ \"name\": \"John Doe\", \"age\": 30, \"isEmployed\": true }";

        Struct resultStruct = StructHelper.convertJsonToStruct(schema, jsonString);

        assertNotNull(resultStruct);
        assertEquals("John Doe", resultStruct.get("name"));
        assertEquals(30, resultStruct.get("age"));
        assertEquals(true, resultStruct.get("isEmployed"));
    }

    @Test
    void testConvertJsonToStructWithMissingOptionalField() throws IOException {
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA) // age is optional
                .field("isEmployed", Schema.BOOLEAN_SCHEMA)
                .build();

        String jsonString = "{ \"name\": \"Jane Doe\", \"isEmployed\": false }";

        Struct resultStruct = StructHelper.convertJsonToStruct(schema, jsonString);

        assertNotNull(resultStruct);
        assertEquals("Jane Doe", resultStruct.get("name"));
        assertNull(resultStruct.get("age")); // Age is missing, should be null
        assertEquals(false, resultStruct.get("isEmployed"));
    }

    @Test
    void testConvertJsonToStructWithNestedStruct() throws IOException {
        Schema addressSchema = SchemaBuilder.struct()
                .field("street", Schema.STRING_SCHEMA)
                .field("city", Schema.STRING_SCHEMA)
                .build();

        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("address", addressSchema)  // Nested address struct
                .build();

        String jsonString = "{ \"name\": \"John Doe\", \"age\": 40, \"address\": { \"street\": \"123 Main St\", \"city\": \"New York\" } }";

        Struct resultStruct = StructHelper.convertJsonToStruct(schema, jsonString);

        assertNotNull(resultStruct);
        assertEquals("John Doe", resultStruct.get("name"));
        assertEquals(40, resultStruct.get("age"));

        Struct addressStruct = resultStruct.getStruct("address");
        assertNotNull(addressStruct);
        assertEquals("123 Main St", addressStruct.get("street"));
        assertEquals("New York", addressStruct.get("city"));
    }

    @Test
    void testConvertJsonToStructWithArray() throws IOException {
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("numbers", SchemaBuilder.array(Schema.INT32_SCHEMA).build())  // Array of integers
                .build();

        String jsonString = "{ \"name\": \"John Doe\", \"numbers\": [1, 2, 3, 4, 5] }";

        Struct resultStruct = StructHelper.convertJsonToStruct(schema, jsonString);

        assertNotNull(resultStruct);
        assertEquals("John Doe", resultStruct.get("name"));

        List<Integer> numbers = resultStruct.getArray("numbers");
        assertNotNull(numbers);
        assertEquals(5, numbers.size());
        assertEquals(List.of(1, 2, 3, 4, 5), numbers);
    }

    @Test
    void testConvertJsonToStructWithMap() throws IOException {
        Schema schema = SchemaBuilder.struct()
                .field("attributes", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .build();

        String jsonString = "{ \"attributes\": { \"height\": 180, \"weight\": 75 } }";

        Struct resultStruct = StructHelper.convertJsonToStruct(schema, jsonString);

        assertNotNull(resultStruct);

        Map<String, Integer> attributes = resultStruct.getMap("attributes");
        assertNotNull(attributes);
        assertEquals(2, attributes.size());
        assertEquals(180, attributes.get("height"));
        assertEquals(75, attributes.get("weight"));
    }
}
