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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class JRCommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(JRSourceTask.class);
    
    public static List<String> templates() {
        List<String> templates = new ArrayList<>();
        
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", "jr list");
        
        try {
            Process process = processBuilder.start();
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                String tmpLine = line.trim();
                if(!tmpLine.isEmpty() && !containsWhitespace(tmpLine) && tmpLine.length() > 4) {
                    //first 4 chars to be escaped (ANSI color)
                    templates.add(tmpLine.substring(4));
                }
            }

            printError(process);

        } catch (Exception e) {
            if (LOG.isErrorEnabled())
                LOG.error("JR command failed:{}", e.getMessage());
        }
        return templates; 
    }

    public static String runTemplate(String cmd) {

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("bash", "-c", "jr run " + cmd);

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
                LOG.error("JR command failed:{}", e.getMessage());
        }
        assert output != null;
        return output.toString();
    }

    public static void printError(Process process) throws Exception {
        int exitVal = process.waitFor();
        if (exitVal != 0)  {
            BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            StringBuilder errorOutput = new StringBuilder();
            String line;
            while ((line = errorReader.readLine()) != null) {
                errorOutput.append(line).append("\n");
            }
            if (LOG.isErrorEnabled())
                LOG.error("JR command failed:{}", errorOutput);
        }
    }

    public static boolean containsWhitespace(String str) {
        return str.matches(".*\\s.*");
    }
}
