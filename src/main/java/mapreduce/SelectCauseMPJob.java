package mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by zb on 2018/12/9.
 */
public class SelectCauseMPJob extends Configured implements Tool{


     public static class SelectCauseMapper extends Mapper<LongWritable,Text,NullWritable,Text> {

         @Override
         protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             super.map(key, value, context);
         }
     }

    public int run(String[] strings) throws Exception {
        return 0;
    }
}
