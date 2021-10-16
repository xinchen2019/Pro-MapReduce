package com.apple.mapreduce.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Program: Pro-MapReduce
 * @ClassName: FlowDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-28 01:03
 * @Version 1.1.0
 **/
public class FlowDriver {
    public static void main(String[] args) throws Exception {
        // 1 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 获取jar的存储路径
        job.setJarByClass(FlowDriver.class);

        // 3 关联map和reduce的class类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4 设置map阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5 设置最后输出数据的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 8设置分区
        job.setPartitionerClass(FlowPartitioner.class);
        // 9 同时指定相应数量的reduce task
        job.setNumReduceTasks(1);

        // 6 设置输入数据的路径 和输出数据的路径
        //"file:///C:\\Users\\13128\\Desktop\\7.txt";
        String inputPath = "data\\phonedata\\phone_data.txt";
        String outputPath = "output\\phonedata\\phone_data";

        FileInputFormat.setInputPaths(job, new Path(inputPath));

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(outputPath);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
