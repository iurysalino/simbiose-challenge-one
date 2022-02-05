import java.io.IOException;
import java.util.List;

import org.apache.arrow.vector.VectorSchemaRoot;

public class CsvFileReader {
  public static void main(String[] args) throws IOException {
    FileManipulation fm = new FileManipulation();
    List lista = fm.csvFileReader(args[0]);
    VectorSchemaRoot v = fm.vectorSchemaRootPopulate(lista);
    fm.writeInCsv(v, args[1]);
  }
}
