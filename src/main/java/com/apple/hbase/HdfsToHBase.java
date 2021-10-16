package com.apple.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: HdfsToHBase
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-05 16:50
 * @Version 1.1.0
 * 参考
 **/
public class HdfsToHBase {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{
                "hdfs://master:9000/HbaseToHdfs1"
        };
        //将结果存入hbase的表名
        String tableName = "apple:mytb2";
        //Configuration conf = new Configuration();
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        createHBaseTable(tableName, conf);
        String input = args[0];
        Job job = Job.getInstance(conf, HdfsToHBase.class.getSimpleName());
        //Job job = new Job(conf, "WordCount table with" + input);

        job.setJarByClass(HdfsToHBase.class);
        job.setNumReduceTasks(3);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void createHBaseTable(String tableName, Configuration conf)
            throws IOException {

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

        HColumnDescriptor columnDesc = new HColumnDescriptor("cf");
        tableDesc.addFamily(columnDesc);
        Connection conn = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("table exists,trying to recreate table");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        System.out.println("create new table:" + tableName);
        admin.createTable(tableDesc);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable i = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] strs = value.toString().trim().split("/n");
            for (String m : strs) {
                context.write(new Text(m), i);
            }
        }
    }

    public static class Reduce extends TableReducer<Text, IntWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(
                    Bytes.toBytes("cf"),
                    Bytes.toBytes("count"),
                    Bytes.toBytes(String.valueOf(sum))
            );
            context.write(NullWritable.get(), put);
        }
    }
}
