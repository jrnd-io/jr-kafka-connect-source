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

package io.jrnd.kafka.connect.format.protobuf;

import io.jrnd.kafka.connect.connector.format.StructHelper;
import io.jrnd.kafka.connect.connector.format.protobuf.ProtobufHelper;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class ProtobufHelperTest {

    @Test
    public void testCreateProtobufSchemaFromJson_simpleFields() throws Exception {
        String jsonString = "{ \"name\": \"John\", \"age\": 30, \"isEmployee\": true }";
        String messageName = "Person";

        Schema schema = ProtobufHelper.createProtobufSchemaFromJson(messageName, jsonString);

        assertNotNull(schema);
        assertEquals("Person", schema.name());
        assertEquals(3, schema.fields().size());

        assertEquals(Schema.Type.STRING, schema.field("name").schema().type());
        assertEquals(Schema.Type.INT32, schema.field("age").schema().type());
        assertEquals(Schema.Type.BOOLEAN, schema.field("isEmployee").schema().type());
    }

    @Test
    public void testCreateProtobufSchemaFromJson_invalidJson() {

        String invalidJsonString = "{ \"name\": \"John\", \"age\": }";
        String messageName = "Person";

        assertThrows(Exception.class, () -> {
            ProtobufHelper.createProtobufSchemaFromJson(messageName, invalidJsonString);
        });
    }

}
