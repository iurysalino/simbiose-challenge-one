import java.io.IOException;
import java.util.List;

import org.apache.arrow.vector.VectorSchemaRoot;

public class CsvFileReader {
  public static void main(String[] args) throws IOException {
    FileManipulation fileManipulation = new FileManipulation();
    List listDataCvsReader = fileManipulation.csvFileReader(args[0]);
    try (VectorSchemaRoot vectorSchemaRoot = fileManipulation.vectorSchemaRootPopulate(listDataCvsReader)) {
      fileManipulation.writeInCsv(vectorSchemaRoot, args[1]);
    }
  }
}
