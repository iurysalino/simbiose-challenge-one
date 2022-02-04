import java.io.IOException;
import java.util.Scanner;

public class CsvFileReader {

  private static VectorSchemaRootManipulation vm = new VectorSchemaRootManipulation();
  public static void main(String[] args) throws IOException {
    String path = "src/mlb_players.csv";
    vm.csv(path);
  }
}
