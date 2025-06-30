package io;

import java.io.IOException;
import java.io.Serializable;

/** 一个序列化修饰器，继承此写取出器即可拥有 store 和 load 方法 */
public abstract class Storable implements Serializable {

  private static final long serialVersionUID = 1L;

  public void store(String dest) throws IOException {
    IOUtils.writeObject(this, dest);
  }

  public static <T> T load(String src) throws IOException, ClassNotFoundException {
    return (T) IOUtils.readObject(src);
  }
}
