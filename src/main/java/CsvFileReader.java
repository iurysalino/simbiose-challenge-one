import java.io.IOException;

public class CsvFileReader {
  public static void main(String[] args) throws IOException {
    new FileManipulation().csvFileReader(args[0]);
  }
}
