import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class FileManipulation {
  private String row;

  public FileManipulation() {
  }

  public void csvFileReader(String path) {
    try(BufferedReader csvReader = new BufferedReader(new FileReader(path))) {
      while ((row = csvReader.readLine()) != null) {
        String[] data = row.split(",");
        for (String index : data) {
          System.out.printf("%-10s", index);
        }
        System.out.println();
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
