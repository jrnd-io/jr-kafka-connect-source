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

package io.jrnd.kafka.connect.connector;

import io.jrnd.kafka.connect.connector.model.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class JRCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(JRCommandExecutor.class);

    private static final String JR_EXECUTABLE_NAME = "jr";
    private static final String JR_OUTPUT_TEMPLATE_FORMAT = "'{{.K}}{{.V}}'";
    private static String executablePath;

    private JRCommandExecutor() {}

    private static class JRCommandExecutorHelper {
        private static final JRCommandExecutor INSTANCE = new JRCommandExecutor();
    }

    public static JRCommandExecutor getInstance(String executablePath) {
        JRCommandExecutor.executablePath = executablePath;
        return JRCommandExecutorHelper.INSTANCE;
    }


    public List<String> templates() {
        List<String> templates = new ArrayList<>();
        
        ProcessBuilder processBuilder = new ProcessBuilder();

        StringBuilder commandBuilder = new StringBuilder();
        if(executablePath != null && !executablePath.isEmpty()) {
            commandBuilder.append(executablePath).append(File.separator);
        }
        commandBuilder.append(JR_EXECUTABLE_NAME);
        commandBuilder.append(" list");
        commandBuilder.append(" -n");

        processBuilder.command(
                CommandInterpeter.getInstance().getCommand(),
                CommandInterpeter.getInstance().getArguments(),
                commandBuilder.toString());
        
        try {
            Process process = processBuilder.start();
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                String tmpLine = line.trim();
                if(!tmpLine.isEmpty() && !containsWhitespace(tmpLine)) {
                    templates.add(tmpLine);
                }
            }

            printError(process);

        } catch (Exception e) {
            if (LOG.isErrorEnabled())
                LOG.error(JR_EXECUTABLE_NAME + " command failed:{}", e.getMessage());
        }
        return templates; 
    }

    public List<String> runTemplate(
            Template templateWrapper,
            int objects,
            String keyField,
            int keyValueLength) {

        ProcessBuilder processBuilder = new ProcessBuilder();

        StringBuilder commandBuilder = new StringBuilder();
        if(executablePath != null && !executablePath.isEmpty()) {
            commandBuilder.append(executablePath).append(File.separator);
        }
        commandBuilder.append(JR_EXECUTABLE_NAME);

        // Case: key with embedded template
        if(templateWrapper.getKeyTemplate() != null && templateWrapper.getKeyTemplate().isEmpty()) {
            commandBuilder.append(" run ");
            commandBuilder.append(templateWrapper.isEmbedded()? "--embedded '" + templateWrapper.getTemplate() + "'":templateWrapper.getTemplate());
            commandBuilder.append(" --key '{{ ");
            commandBuilder.append(templateWrapper.getKeyTemplate());
            commandBuilder.append(" }}'");
            commandBuilder.append(" --outputTemplate ");
            commandBuilder.append(JR_OUTPUT_TEMPLATE_FORMAT);
            commandBuilder.append(" -n ");
            commandBuilder.append(objects);
        }
        // Case: no key field and no key embedded template
        else if(keyField == null || keyField.isEmpty()) {
            commandBuilder.append(" run ");
            commandBuilder.append(templateWrapper.isEmbedded()? "--embedded '" + templateWrapper.getTemplate() + "'":templateWrapper.getTemplate());
            commandBuilder.append(" -n ");
            commandBuilder.append(objects);
        }
        // Case: key field and no key embedded template
        else {
            commandBuilder.append(" run ");
            commandBuilder.append(templateWrapper.isEmbedded()? "--embedded '" + templateWrapper.getTemplate() + "'":templateWrapper.getTemplate());
            commandBuilder.append(" --key '{{key " + "\"{\\\"");
            commandBuilder.append(keyField);
            commandBuilder.append("\\\":\" ");
            commandBuilder.append(keyValueLength);
            commandBuilder.append("}");
            commandBuilder.append("}}' --outputTemplate ");
            commandBuilder.append(JR_OUTPUT_TEMPLATE_FORMAT);
            commandBuilder.append(" -n ");
            commandBuilder.append(objects);
        }

        if (LOG.isDebugEnabled())
            LOG.debug("JR command to execute {}", commandBuilder);

        processBuilder.command(
                CommandInterpeter.getInstance().getCommand(),
                CommandInterpeter.getInstance().getArguments(),
                commandBuilder.toString());

        StringBuilder output = null;
        try {
            Process process = processBuilder.start();
            
            output = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            printError(process);

        } catch (Exception e) {
            if (LOG.isErrorEnabled())
                LOG.error(JR_EXECUTABLE_NAME + " command failed:{}", e.getMessage());
        }
        assert output != null;
        return splitJsonObjects(output.toString().replaceAll("\\r?\\n", ""));
    }

    private void printError(Process process) throws Exception {
        int exitVal = process.waitFor();
        if (exitVal != 0)  {
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            StringBuilder errorOutput = new StringBuilder();
            String line;
            while ((line = errorReader.readLine()) != null) {
                errorOutput.append(line).append("\n");
            }
            if (LOG.isErrorEnabled())
                LOG.error(JR_EXECUTABLE_NAME + " command failed:{}", errorOutput);
        }
    }

    private boolean containsWhitespace(String str) {
        return str.matches(".*\\s.*");
    }

    private List<String> splitJsonObjects(String jsonString) {
        List<String> jsonObjects = new ArrayList<>();
        int braceCount = 0;
        StringBuilder currentJson = new StringBuilder();

        for (char c : jsonString.toCharArray()) {
            if (c == '{') {
                if (braceCount == 0 && !currentJson.isEmpty()) {
                    jsonObjects.add(currentJson.toString());
                    currentJson.setLength(0);
                }
                braceCount++;
            }
            if (c == '}') {
                braceCount--;
            }
            currentJson.append(c);
            if (braceCount == 0 && !currentJson.isEmpty()) {
                jsonObjects.add(currentJson.toString());
                currentJson.setLength(0);
            }
        }
        return jsonObjects;
    }

    private static class CommandInterpeter {
        private String command = "bash";
        private String arguments = "-c";

        private static class CommandInterpeterHelper {
            private static final CommandInterpeter INSTANCE = new CommandInterpeter();
        }

        public static CommandInterpeter getInstance() {
            return CommandInterpeterHelper.INSTANCE;
        }

        private CommandInterpeter() {

            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                this.command = "cmd.exe";
                this.arguments = "/c";
            }
        }

        public String getCommand() {
            return command;
        }

        public String getArguments() {
            return arguments;
        }

    }
}
