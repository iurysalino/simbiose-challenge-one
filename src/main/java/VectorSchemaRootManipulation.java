import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.arrow.memory.RootAllocator;

public class VectorSchemaRootManipulation {
  private static String row;
  private static BufferedReader csvReader = null;
  private RootAllocator rootAllocator = new RootAllocator();

  public void csv(String path) throws IOException {
    try {
      csvReader = new BufferedReader(new FileReader(path));
      while ((row = csvReader.readLine()) != null) {
        String[] data = row.split(",");

        for (String index : data) {
          System.out.printf("%-10s", index);
        }
        System.out.println();
      }
    } finally {
      csvReader.close();
    }
  }
}
