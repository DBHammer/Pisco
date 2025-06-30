package gen.schema;

import java.io.IOException;
import org.junit.Test;

public class SchemaTest {

  @Test
  public void load() throws IOException, ClassNotFoundException {
    String src = "./out/schema/schema.obj";
    Schema schema = Schema.load(src);
    System.out.println(schema);
  }
}
