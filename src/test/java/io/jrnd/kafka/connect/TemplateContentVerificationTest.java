package io.jrnd.kafka.connect;

import io.jrnd.kafka.connect.connector.JRSourceConnector;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify the actual content reading from classpath templates
 */
public class TemplateContentVerificationTest {

    @Test
    public void testReadTemplateContentFromClasspath() throws Exception {
        JRSourceConnector connector = new JRSourceConnector();
        
        // Use reflection to test the private readTemplate method
        Method readTemplateMethod = JRSourceConnector.class.getDeclaredMethod("readTemplate", String.class);
        readTemplateMethod.setAccessible(true);
        
        // Test reading sample_user.json from classpath
        String templateContent = (String) readTemplateMethod.invoke(connector, "classpath:templates/sample_user.json");
        
        assertNotNull(templateContent);
        assertFalse(templateContent.isEmpty());
        
        // Verify that the content contains expected fields from our sample template
        assertTrue(templateContent.contains("user_id"));
        assertTrue(templateContent.contains("username"));
        assertTrue(templateContent.contains("email"));
        assertTrue(templateContent.contains("{{uuid}}"));
        assertTrue(templateContent.contains("{{username}}"));
        assertTrue(templateContent.contains("{{email}}"));
        
        System.out.println("Successfully read template content: " + templateContent);
    }

    @Test
    public void testReadProductTemplateContentFromClasspath() throws Exception {
        JRSourceConnector connector = new JRSourceConnector();
        
        // Use reflection to test the private readTemplate method
        Method readTemplateMethod = JRSourceConnector.class.getDeclaredMethod("readTemplate", String.class);
        readTemplateMethod.setAccessible(true);
        
        // Test reading simple_product.json from classpath
        String templateContent = (String) readTemplateMethod.invoke(connector, "classpath:templates/simple_product.json");
        
        assertNotNull(templateContent);
        assertFalse(templateContent.isEmpty());
        
        // Verify that the content contains expected fields from our product template
        assertTrue(templateContent.contains("product_id"));
        assertTrue(templateContent.contains("name"));
        assertTrue(templateContent.contains("price"));
        assertTrue(templateContent.contains("{{integer"));
        assertTrue(templateContent.contains("{{randoms"));
        assertTrue(templateContent.contains("{{float"));
        
        System.out.println("Successfully read product template content: " + templateContent);
    }
}