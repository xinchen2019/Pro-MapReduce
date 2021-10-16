package com.apple.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * @Program: Pro-MapReduce
 * @ClassName: HbaseToHdfs1
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-05 15:40
 * @Version 1.1.0
 **/
public class HbaseToHdfs1 {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{
                "hdfs://master:9000/HbaseToHdfs1",
        };

        System.setProperty("HADOOP_USER_NAME", "ubuntu");

        String sourceTable = "hello";

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Job job = Job.getInstance(conf, "WordCountHbaseReader");

        job.setJarByClass(HbaseToHdfs1.class);

        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(
                sourceTable,
                scan,
                dddoMapper.class,
                Text.class,
                IntWritable.class,
                job
        );

        job.setReducerClass(WWordCountHbaseReaderReduce.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        MultipleOutputs.addNamedOutput(
                job,
                "hdfs",
                TextOutputFormat.class,
                WritableComparable.class,
                Writable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class dddoMapper extends TableMapper<Text, IntWritable> {

        IntWritable one = new IntWritable(1);

        Text word = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {

            String[] rowValue = Bytes.toString(CellUtil.cloneValue(value.listCells().get(0))).split(" ");

            for (String str : rowValue) {
                word.set(str);
                context.write(word, one);
            }
        }
    }

    public static class WWordCountHbaseReaderReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable value : values) {
                total++;
            }
            context.write(key, new IntWritable(total));
        }
    }
}
