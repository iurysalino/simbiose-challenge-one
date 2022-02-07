import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class FileManipulation {

  private static int totalRows = 0;
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);


  public List<FieldVector> csvFileReader(String path) throws IOException {
    final VarCharVector vectorName = new VarCharVector("name", allocator);
    final VarCharVector vectorTeam = new VarCharVector("team", allocator);
    final VarCharVector vectorPosition = new VarCharVector("position", allocator);
    final VarCharVector vectorHeight = new VarCharVector("height", allocator);
    final VarCharVector vectorWeight = new VarCharVector("weight", allocator);
    final VarCharVector vectorAge = new VarCharVector("age", allocator);

    String line;
    try (BufferedReader csvReader = new BufferedReader(new FileReader(path))){
      int index = 0;
      while ((line = csvReader.readLine()) != null) {
        String[] data = line.split(",");
        vectorName.setSafe(index, data[0].getBytes());
        vectorTeam.setSafe(index, data[1].getBytes());
        vectorPosition.setSafe(index, data[2].getBytes());
        vectorHeight.setSafe(index, data[3].getBytes());
        vectorWeight.setSafe(index, data[4].getBytes());
        vectorAge.setSafe(index, data[5].getBytes());
        index++;
        totalRows++;
      }
    }
    return Arrays.asList(vectorName, vectorTeam, vectorPosition, vectorHeight, vectorWeight, vectorAge);
  }

  public VectorSchemaRoot vectorSchemaRootPopulate(List<FieldVector> listDataVectors) {
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(listDataVectors);
    vectorSchemaRoot.setRowCount(totalRows);
    return vectorSchemaRoot;
  }

  private void cellSeparator(FileWriter csvWriter) throws IOException {
    csvWriter.append(",");
  }

  private void newLine(FileWriter csvWriter) throws IOException {
    csvWriter.append("\n");
  }

  public void writeInCsv(VectorSchemaRoot vectorSchemaRoot, String path) {
    VarCharVector vectorNameConsumer = (VarCharVector) vectorSchemaRoot.getVector(0);
    VarCharVector vectorTeamConsumer = (VarCharVector) vectorSchemaRoot.getVector(1);
    VarCharVector vectorPositionConsumer = (VarCharVector) vectorSchemaRoot.getVector(2);
    VarCharVector vectorHeightConsumer = (VarCharVector) vectorSchemaRoot.getVector(3);
    VarCharVector vectorWeightConsumer = (VarCharVector) vectorSchemaRoot.getVector(4);
    VarCharVector vectorAgeConsumer = (VarCharVector) vectorSchemaRoot.getVector(5);

    int count = 0;
    try {
      FileWriter csvWriter = new FileWriter(path);
      while (count <= 1035) {
        csvWriter.append(vectorNameConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(vectorTeamConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(vectorPositionConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(vectorHeightConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(vectorWeightConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(vectorAgeConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        newLine(csvWriter);
        count++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
