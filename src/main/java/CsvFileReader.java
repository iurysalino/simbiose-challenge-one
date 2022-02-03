import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CsvFileReader {

  private static final String cvs = "src/";
  private static String row;
  private static final String file = "mlb_players.csv";
  private static BufferedReader csvReader = null;

  public static void main(String[] args) throws IOException {
    try {
      csvReader = new BufferedReader(new FileReader(cvs+file));
      while ((row = csvReader.readLine()) != null) {
        String[] data = row.split(",");

        for(String index : data ){
          System.out.printf("%-10s", index);
        }
        System.out.println();
      }
    }catch (Exception exception){
      exception.printStackTrace();
    }finally {
      csvReader.close();
    }
  }
}
