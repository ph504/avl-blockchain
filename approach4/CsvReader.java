package approach4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CsvReader {
    private String[] header = null;
    public ArrayList<Map<String,String>> data = null;

    public CsvReader(String filePath) {

        String line;
        boolean isHeader = true;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            while ((line = reader.readLine()) != null) {
                String[] keyValuePair = line.replace("\"","").split(",");
                if (isHeader) {
                    this.header = keyValuePair;
                    this.data = new ArrayList<>();
                    isHeader = false;
                    continue;
                }

                if (keyValuePair.length != this.header.length) {
                    throw new Exception("header length (" + this.header.length + ") != line split length (" + keyValuePair.length + ")");
                }

                Map<String,String> map = new HashMap<>();
                for (int i=0; i< this.header.length;i++) {
                    String key = this.header[i];
                    String value = keyValuePair[i];
                    map.put(key, value);
                }
                this.data.add(map);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }
}
