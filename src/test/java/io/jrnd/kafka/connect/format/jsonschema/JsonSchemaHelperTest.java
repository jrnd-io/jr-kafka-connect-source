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

package io.jrnd.kafka.connect.format.jsonschema;

import io.jrnd.kafka.connect.connector.format.jsonschema.JsonSchemaHelper;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class JsonSchemaHelperTest {

    @Test
    public void testCreateJsonSchemaFromJson_simpleTypes() throws Exception {
        // Test a simple JSON with various data types
        String jsonDocument = """
            {
                "name": "John Doe",
                "age": 30,
                "isEmployee": true,
                "salary": 55000.50
            }
            """;

        Schema schema = JsonSchemaHelper.createJsonSchemaFromJson(jsonDocument);

        assertNotNull(schema);
        assertEquals(Schema.Type.STRUCT, schema.type());
        assertEquals(4, schema.fields().size());

        assertEquals(Schema.Type.STRING, schema.field("name").schema().type());
        assertEquals(Schema.Type.FLOAT64, schema.field("salary").schema().type());
        assertEquals(Schema.Type.BOOLEAN, schema.field("isEmployee").schema().type());
        assertEquals(Schema.Type.FLOAT64, schema.field("age").schema().type());
    }

    @Test
    public void testCreateJsonSchemaFromJson_nestedObject() throws Exception {

        String jsonDocument = """
            {
                "name": "Jane Doe",
                "address": {
                    "street": "Main St",
                    "city": "Metropolis",
                    "zipcode": 12345
                }
            }
            """;

        Schema schema = JsonSchemaHelper.createJsonSchemaFromJson(jsonDocument);

        assertNotNull(schema);
        assertEquals(Schema.Type.STRUCT, schema.type());

        assertEquals(Schema.Type.STRING, schema.field("name").schema().type());

        Schema addressSchema = schema.field("address").schema();
        assertNotNull(addressSchema);
        assertEquals(Schema.Type.STRUCT, addressSchema.type());

        assertEquals(Schema.Type.STRING, addressSchema.field("street").schema().type());
        assertEquals(Schema.Type.STRING, addressSchema.field("city").schema().type());
        assertEquals(Schema.Type.FLOAT64, addressSchema.field("zipcode").schema().type());
    }

    @Test
    public void testCreateJsonSchemaFromJson_array() throws Exception {

        String jsonDocument = """
            {
                "name": "Jane Doe",
                "skills": ["Java", "Kafka", "Docker"]
            }
            """;

        Schema schema = JsonSchemaHelper.createJsonSchemaFromJson(jsonDocument);

        assertNotNull(schema);
        assertEquals(Schema.Type.STRUCT, schema.type());

        Schema skillsSchema = schema.field("skills").schema();
        assertNotNull(skillsSchema);
        assertEquals(Schema.Type.ARRAY, skillsSchema.type());

        Schema elementType = skillsSchema.valueSchema();
        assertEquals(Schema.Type.STRING, elementType.type());
    }

    @Test
    public void testCreateJsonSchemaFromJson_invalidJson() {

        String jsonDocument = """
            {
                "name": "John Doe",
                "age": 
            }
            """;

        assertThrows(IOException.class, () -> {
            JsonSchemaHelper.createJsonSchemaFromJson(jsonDocument);
        });
    }
}
