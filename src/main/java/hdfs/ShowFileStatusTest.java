package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 遍历文件文件元数据 =》 hadoop fs -lsr /
 * Created by zb on 2018/12/6.
 */
public class ShowFileStatusTest {
   public static void main(String[] args ) throws  Exception{
       String path =args[0];
       Configuration configuration = new Configuration();
       FileSystem fileSystem = FileSystem.get(java.net.URI.create(path), configuration);
       Path[] paths = new Path[args.length];
       for(int i= 0;i<paths.length;i++ ) {
           paths[i] =  new Path(args[i]);
       }

       FileStatus[] fileStatus = fileSystem.listStatus(paths);
       for(FileStatus fileStatus1 :fileStatus) {
           System.out.println(fileStatus1);
       }


   }
}
