package transform;

import gen.data.generator.PKGenerator;
import gen.data.param.PKParam;
import gen.data.value.AttrValue;
import gen.schema.Schema;
import gen.schema.table.Attribute;
import gen.schema.table.Table;
import gen.shadow.DBMirror;
import gen.shadow.TableMirror;
import io.ObjectCollection;
import java.io.IOException;
import org.junit.Test;

public class LogicalTransform {
  @Test
  public void test() throws IOException, ClassNotFoundException {
    String inFile = "out/ObjectCollection.obj";
    ObjectCollection objectCollection = ObjectCollection.load(inFile);

    Schema schema = objectCollection.getSchema();
    DBMirror shadow = new DBMirror(objectCollection.getMirrorData());

    for (Table table : schema.getTableList()) {
      TableMirror tableMirror = shadow.getTableMirrorById(table.getTableId());
      for (Attribute pkAttr : table.getPrimaryKey()) {
        PKParam pkParam = tableMirror.getPkParamById(pkAttr.getAttrId());
        System.out.printf("%s - %s%n", table.getTableName(), pkAttr.getAttrName());
        for (int pkId = 0; pkId < table.getTableSize(); pkId++) {
          AttrValue attrValue = PKGenerator.pkId2Pk(pkId, pkAttr.getAttrType(), pkParam);
          int logicalValue = PKGenerator.pk2PkId(attrValue, pkParam);

          if (logicalValue != pkId) {
            System.err.printf("%s/%s/%s%n", pkId, logicalValue, attrValue.value);
          }

          System.out.printf("%s/%s/%s%n", pkId, logicalValue, attrValue.value);
        }
      }
    }
  }

  @Test
  public void test2() throws IOException, ClassNotFoundException {
    String inFile = "out/ObjectCollection.obj";
    ObjectCollection objectCollection = ObjectCollection.load(inFile);

    Schema schema = objectCollection.getSchema();
    DBMirror shadow = new DBMirror(objectCollection.getMirrorData());

    for (Table table : schema.getTableList()) {
      TableMirror tableMirror = shadow.getTableMirrorById(table.getTableId());

      for (int pkId = 0; pkId < table.getTableSize(); pkId++) {

        for (Attribute pkAttr : table.getPrimaryKey()) {
          PKParam pkParam = tableMirror.getPkParamById(pkAttr.getAttrId());
          AttrValue attrValue = PKGenerator.pkId2Pk(pkId, pkAttr.getAttrType(), pkParam);
          int logicalValue = PKGenerator.pk2PkId(attrValue, pkParam);

          if (logicalValue != pkId) {
            System.err.printf("%s/%s/%s%n", pkId, logicalValue, attrValue.value);
          }

          System.out.printf("%s/%s/%s%n", pkId, logicalValue, attrValue.value);
        }
      }
    }
  }
}
