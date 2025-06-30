package io;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import lombok.Cleanup;

public class IOUtils {

  /**
   * 输出一个对象到指定的路径下，如果路径不存在，会自动创建路径
   *
   * @param object 要序列化的对象
   * @param dest 目标位置
   */
  public static void writeObject(Object object, String dest) throws IOException {
    File file = new File(dest);
    File parentFile = file.getParentFile();

    // 首先确保文件和目录都存在
    if (!parentFile.exists()) parentFile.mkdirs();

    if (!file.exists()) file.createNewFile();

    @Cleanup ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(file.toPath()));
    oos.writeObject(object);
  }

  /**
   * 从制定路径下，读取一个object出来
   *
   * @param src 要反序列化的对象输出
   * @return Object of <T>
   */
  public static Object readObject(String src) throws IOException, ClassNotFoundException {
    @Cleanup ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Paths.get(src)));
    return ois.readObject();
  }

  /**
   * 输出一个string到指定路径下，形成一个文本文件
   *
   * @param string 要输出的String
   * @param targetPath dest
   * @param isAppend isAppend
   */
  public static void writeString(String string, String targetPath, boolean isAppend)
      throws IOException {
    File file = new File(targetPath);
    File parentFile = file.getParentFile();

    // 首先确保文件和目录都存在
    if (!parentFile.exists()) parentFile.mkdirs();
    if (!file.exists()) file.createNewFile();

    @Cleanup
    BufferedWriter bw =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, isAppend)));

    bw.write(string);
    bw.flush();
  }

  /**
   * 指定路径下从指定地址的文本文件中读取数据，以每行一个String的形式组织
   *
   * @param targetPath dest
   */
  public static List<String> readString(String targetPath) throws IOException {
    File file = new File(targetPath);
    File parentFile = file.getParentFile();

    // 首先确保文件和目录都存在
    if (!parentFile.exists() || !file.exists()) return null;

    @Cleanup
    BufferedReader br =
        new BufferedReader(new InputStreamReader(Files.newInputStream(file.toPath())));

    List<String> result = new ArrayList<>();
    String readLine = br.readLine();
    while (readLine != null) {
      result.add(readLine);
      readLine = br.readLine();
    }
    return result;
  }
}
