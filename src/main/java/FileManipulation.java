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

  private int totalRows;
  public static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public List<FieldVector> csvFileReader(String path) throws IOException {
    final VarCharVector name = new VarCharVector("name", allocator);
    final VarCharVector team = new VarCharVector("team", allocator);
    final VarCharVector position = new VarCharVector("position", allocator);
    final VarCharVector height = new VarCharVector("height", allocator);
    final VarCharVector weight = new VarCharVector("weight", allocator);
    final VarCharVector age = new VarCharVector("age", allocator);

    String line;
    try (BufferedReader csvReader = new BufferedReader(new FileReader(path))) {
      totalRows = 0;
      while ((line = csvReader.readLine()) != null) {
        String[] data = line.split(",");
        name.setSafe(totalRows, data[0].getBytes());
        team.setSafe(totalRows, data[1].getBytes());
        position.setSafe(totalRows, data[2].getBytes());
        height.setSafe(totalRows, data[3].getBytes());
        weight.setSafe(totalRows, data[4].getBytes());
        age.setSafe(totalRows, data[5].getBytes());
        totalRows++;
      }
    }
    return Arrays.asList(name, team, position, height, weight, age);
  }

  public VectorSchemaRoot vectorSchemaRootPopulate(List<FieldVector> listDataVectors) {
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(listDataVectors);
    vectorSchemaRoot.setRowCount(totalRows);
    return vectorSchemaRoot;
  }

  private void cellSeparator(FileWriter csvWriter) throws IOException {
    csvWriter.append(",");
  }

  private void createNewLine(FileWriter csvWriter) throws IOException {
    csvWriter.append("\n");
  }

  public void writeInCsv(VectorSchemaRoot vectorSchemaRoot, String path) throws IOException {
    VarCharVector nameConsumer = (VarCharVector) vectorSchemaRoot.getVector(0);
    VarCharVector teamConsumer = (VarCharVector) vectorSchemaRoot.getVector(1);
    VarCharVector positionConsumer = (VarCharVector) vectorSchemaRoot.getVector(2);
    VarCharVector heightConsumer = (VarCharVector) vectorSchemaRoot.getVector(3);
    VarCharVector weightConsumer = (VarCharVector) vectorSchemaRoot.getVector(4);
    VarCharVector ageConsumer = (VarCharVector) vectorSchemaRoot.getVector(5);

    int count = 0;
    try (FileWriter csvWriter = new FileWriter(path)) {
      while (count <= totalRows) {
        csvWriter.append(nameConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(teamConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(positionConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(heightConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(weightConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(ageConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        createNewLine(csvWriter);
        count++;
      }
    }
  }
}
