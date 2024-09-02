package io.jrnd.kafka.connect;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

import io.jrnd.kafka.connect.connector.JRCommandExecutor;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.util.List;

public class JRCommandExecutorTest {

    @InjectMocks
    private JRCommandExecutor jrCommandExecutor;

    @Mock
    private Process mockProcess;

    @Mock
    private BufferedReader mockBufferedReader;

    public JRCommandExecutorTest() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testTemplates() throws Exception {
        String mockOutput = "csv_product\n" +
                "csv_user\n" +
                "finance_stock_trade\n" +
                "fleet_mgmt_sensors\n" +
                "fleetmgmt_description\n" +
                "fleetmgmt_location\n" +
                "fleetmgmt_sensor\n" +
                "gaming_game\n" +
                "gaming_player\n" +
                "gaming_player_activity\n" +
                "insurance_customer\n" +
                "insurance_customer_activity\n" +
                "insurance_offer\n" +
                "inventorymgmt_inventory\n" +
                "inventorymgmt_product\n" +
                "iot_device_information\n" +
                "marketing_campaign_finance\n" +
                "net_device\n" +
                "payment_credit_card\n" +
                "payment_transaction\n" +
                "payroll_bonus\n" +
                "payroll_employee\n" +
                "payroll_employee_location\n" +
                "pizzastore_order\n" +
                "pizzastore_order_cancelled\n" +
                "pizzastore_order_completed\n" +
                "pizzastore_util\n" +
                "shoestore_clickstream\n" +
                "shoestore_customer\n" +
                "shoestore_order\n" +
                "shoestore_shoe\n" +
                "shopping_order\n" +
                "shopping_purchase\n" +
                "shopping_rating\n" +
                "siem_log\n" +
                "store\n" +
                "syslog_log\n" +
                "user\n" +
                "user_with_key\n" +
                "users\n" +
                "users_array_map\n" +
                "util_ip\n" +
                "util_userid\n" +
                "webanalytics_clickstream\n" +
                "webanalytics_code\n" +
                "webanalytics_page_view\n" +
                "webanalytics_user";
        when(mockProcess.getInputStream()).thenReturn(new ByteArrayInputStream(mockOutput.getBytes()));
        when(mockProcess.waitFor()).thenReturn(0);

        List<String> result = JRCommandExecutor.templates();

        assertEquals(47, result.size());
        assertTrue(result.contains("users_array_map"));
        assertTrue(result.contains("webanalytics_page_view"));
    }

    @Test
    public void testRunTemplate() throws Exception {
        String mockJsonOutput = "{\"VLAN\":\"ALPHA\"}\n{\"VLAN\":\"GAMMA\"}\n";
        when(mockProcess.getInputStream()).thenReturn(new ByteArrayInputStream(mockJsonOutput.getBytes()));
        when(mockProcess.waitFor()).thenReturn(0);

        List<String> result = JRCommandExecutor.runTemplate("net_device", 2);

        assertEquals(2, result.size());
    }
}
