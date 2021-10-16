package com.apple.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: MRHBase2
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-06 14:26
 * @Version 1.1.0
 * 参考链接：https://blog.csdn.net/qq_41851454/article/details/79781099
 **/
public class MRHBase2 extends Configured implements Tool {

    String inputPath = "data\\student\\student.txt";
    String tableName = "apple:student";

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new MRHBase2(), args);
        System.exit(status);

    }

    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        System.setProperty("HADOOP_USER_NAME", "ubuntu");
        Job job = Job.getInstance(conf, "MRHBase2");
        job.setJarByClass(MRHBase2.class);

        job.setMapperClass(MRHBase2Mapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TableMapReduceUtil.initTableReducerJob(
                tableName,
                MRHBase2Reducer.class,
                job,
                null,
                null,
                null,
                null,
                false);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        boolean isdone = job.waitForCompletion(true);
        return isdone ? 0 : 1;
    }

    public static class MRHBase2Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class MRHBase2Reducer extends TableReducer<Text, NullWritable, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            String[] split = key.toString().split(",");
            Put put = new Put(split[0].getBytes());
            put.addColumn("info".getBytes(), "name".getBytes(), split[1].getBytes());
            put.addColumn("info".getBytes(), "age".getBytes(), split[3].getBytes());
            put.addColumn("info".getBytes(), "sex".getBytes(), split[2].getBytes());
            put.addColumn("info".getBytes(), "department".getBytes(), split[4].getBytes());
            System.out.println(put);
            context.write(NullWritable.get(), put);
        }
    }
}
