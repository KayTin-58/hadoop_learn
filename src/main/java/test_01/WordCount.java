package test_01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

/**
 * Created by zb on 2018/12/1.
 */
public class WordCount {


    /**
     * map阶段
     */
    public static  class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String w = value.toString();
            context.write(new Text(w),new IntWritable(1));
        }
    }

    /**
     * reduce阶段
     */
    public static class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : values) {
                sum += val.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{
        //第一步：创建一个任务
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(WordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        //输入输出的数据类型（后面细节会将）
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //设置输入输出地址
        FileInputFormat.setInputPaths(job,new Path(args[0])); //可以有多个输入文件
        FileOutputFormat.setOutputPath(job,new Path(args[1])); //输出文件只能有一个

        boolean status = job.waitForCompletion(true);
        if(status) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}
