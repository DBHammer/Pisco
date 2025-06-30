package gen.shadow;

import com.google.gson.Gson;
import gen.data.value.AttrValue;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;

public class DBMirrorTest {

  @Test
  public void getPkValueByPkId() throws IOException, ClassNotFoundException {
    String shadowSrc = "./out/MiniShadow.obj";
    DBMirror dbMirror = new DBMirror(MirrorData.load(shadowSrc));

    Gson gson = new Gson();

    Map<String, AttrValue> attrValueMap = dbMirror.getPkValueByPkId(2, 0);
    System.out.println(gson.toJson(attrValueMap));
  }

  @Test
  public void getFkValueByFkId() throws IOException, ClassNotFoundException {
    String shadowSrc = "./out/MiniShadow.obj";
    DBMirror dbMirror = new DBMirror(MirrorData.load(shadowSrc));

    Gson gson = new Gson();

    Map<String, AttrValue> attrValueMap = dbMirror.getFkValueByFkId(2, 93, "fkAttr0");
    System.out.println(gson.toJson(attrValueMap));
  }

  @Test
  public void getAttrValueByIndex() throws IOException, ClassNotFoundException {
    String shadowSrc = "./out/MiniShadow.obj";
    DBMirror dbMirror = new DBMirror(MirrorData.load(shadowSrc));

    Gson gson = new Gson();

    Map<String, AttrValue> attrValueMap = dbMirror.getAttrValueByIndex(1, 13007, "coAttr2");
    System.out.println(gson.toJson(attrValueMap));
  }
}
