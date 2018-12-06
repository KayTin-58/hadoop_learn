package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * Created by zb on 2018/12/5.
 */
public class FileSystemDoubleCat {

    public static void main(String[] args) throws Exception {
        String uri = args[0];//目标文件在hdfs中的地址
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(java.net.URI.create(uri), configuration);
        FSDataInputStream fsDataInputStream = null;
        try {
            fsDataInputStream = fileSystem.open(new Path(uri));
            org.apache.hadoop.io.IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
            //支持将读指针移到文件中的任意一个绝对位置（高消费操作  不建议使用）
            fsDataInputStream.seek(0);
            org.apache.hadoop.io.IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);

        } catch (Exception ex) {

        } finally {
            org.apache.hadoop.io.IOUtils.closeStream(fsDataInputStream);
        }
    }
}
