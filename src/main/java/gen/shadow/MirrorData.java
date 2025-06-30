package gen.shadow;

import gen.data.param.DataRepo;
import gen.schema.Schema;
import io.Storable;
import java.util.Map;
import lombok.Getter;

@Getter
public class MirrorData extends Storable {

  private final Schema schema;
  // tableId -> TableGenModel
  private final Map<Integer, TableMirror> tableMirrorMap;
  private final DataRepo dataRepo;

  public MirrorData(Schema schema, Map<Integer, TableMirror> tableMirrorMap, DataRepo dataRepo) {
    this.schema = schema;
    this.tableMirrorMap = tableMirrorMap;
    this.dataRepo = dataRepo;
  }
}
