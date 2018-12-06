package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * 将本地文件拷贝到HDFS
 * Created by zb on 2018/12/5.
 */
public class FileCopyWriteProgress {

    public static void main(String[] args) {
        String localPath = args[0];//本地地址
        String hdfsPath = args[1];//hdfs地址

        try {
            InputStream  inputStream = new BufferedInputStream(new FileInputStream(localPath));
            Configuration configuration = new Configuration();
            FileSystem fileSystem = FileSystem.get(URI.create(hdfsPath),configuration);
            //创建一个文件
            FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(hdfsPath), new Progressable() {
                public void progress() {
                    System.out.print('.');
                }
            });

            IOUtils.copyBytes(inputStream,fsDataOutputStream,4096,true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
