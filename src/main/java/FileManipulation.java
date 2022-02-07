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
    final VarCharVector Name = new VarCharVector("name", allocator);
    final VarCharVector Team = new VarCharVector("team", allocator);
    final VarCharVector Position = new VarCharVector("position", allocator);
    final VarCharVector Height = new VarCharVector("height", allocator);
    final VarCharVector Weight = new VarCharVector("weight", allocator);
    final VarCharVector Age = new VarCharVector("age", allocator);

    String line;
    try (BufferedReader csvReader = new BufferedReader(new FileReader(path))){
      int index = 0;
      while ((line = csvReader.readLine()) != null) {
        String[] data = line.split(",");
        Name.setSafe(index, data[0].getBytes());
        Team.setSafe(index, data[1].getBytes());
        Position.setSafe(index, data[2].getBytes());
        Height.setSafe(index, data[3].getBytes());
        Weight.setSafe(index, data[4].getBytes());
        Age.setSafe(index, data[5].getBytes());
        index++;
        totalRows++;
      }
    }
    return Arrays.asList(Name, Team, Position, Height, Weight, Age);
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
    VarCharVector NameConsumer = (VarCharVector) vectorSchemaRoot.getVector(0);
    VarCharVector TeamConsumer = (VarCharVector) vectorSchemaRoot.getVector(1);
    VarCharVector PositionConsumer = (VarCharVector) vectorSchemaRoot.getVector(2);
    VarCharVector HeightConsumer = (VarCharVector) vectorSchemaRoot.getVector(3);
    VarCharVector WeightConsumer = (VarCharVector) vectorSchemaRoot.getVector(4);
    VarCharVector AgeConsumer = (VarCharVector) vectorSchemaRoot.getVector(5);

    int count = 0;
    try {
      FileWriter csvWriter = new FileWriter(path);
      while (count <= totalRows) {
        csvWriter.append(NameConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(TeamConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(PositionConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(HeightConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(WeightConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        csvWriter.append(AgeConsumer.getObject(count).toString());
        cellSeparator(csvWriter);
        newLine(csvWriter);
        count++;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
