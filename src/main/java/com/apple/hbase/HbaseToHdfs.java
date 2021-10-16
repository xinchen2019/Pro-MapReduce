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
import org.apache.hadoop.io.NullWritable;
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
 * @ClassName: HbaseToHdfs
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-05 15:00
 * @Version 1.1.0
 **/
public class HbaseToHdfs {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {


        args = new String[]{
                "hdfs://master:9000/HbaseToHdfs"
        };

        System.setProperty("HADOOP_USER_NAME", "ubuntu");

        String sourceTable = "hello";

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        Job job = Job.getInstance(conf, "WordCountHbaseReader");

        job.setJarByClass(HbaseToHdfs.class);

        Scan scan = new Scan();

        TableMapReduceUtil.initTableMapperJob(
                sourceTable,
                scan,
                ddoMapper.class,
                Text.class,
                Text.class,
                job
        );

        job.setReducerClass(WordCountHbaseReaderReduce.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        //MultipleOutputs输出多个文件
        MultipleOutputs.addNamedOutput(job,
                "hdfs",
                TextOutputFormat.class,
                WritableComparable.class,
                Writable.class
        );

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class ddoMapper extends TableMapper<Text, Text> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {

            String rowValue = Bytes.toString(CellUtil.cloneValue(value.listCells().get(0)));
            context.write(new Text(rowValue), new Text("one"));
        }
    }

    public static class WordCountHbaseReaderReduce extends Reducer<Text, Text, Text, NullWritable> {

        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                result.set(value);
                context.write(key, NullWritable.get());
            }
        }
    }
}
