package hdfs;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;

/**
 * Created by zb on 2018/12/5.
 */
public class FileSystemCat {

    public static void main(String[] args) throws Exception{
        String uri = args[0];//目标文件在hdfs中的地址
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(java.net.URI.create(uri),configuration);
        InputStream inputStream = null;
        try {
            inputStream =fileSystem.open(new Path(uri));
            org.apache.hadoop.io.IOUtils.copyBytes(inputStream,System.out,4096,false);
        }catch (Exception ex) {

        }finally {
            org.apache.hadoop.io.IOUtils.closeStream(inputStream);
        }
    }
}
