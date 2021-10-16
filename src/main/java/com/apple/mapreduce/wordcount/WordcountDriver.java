package com.apple.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Program: Pro-MapReduce
 * @ClassName: WordcountDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-27 11:44
 * @Version 1.1.0
 **/
public class WordcountDriver {

    private static Logger logger = LoggerFactory.getLogger(WordcountDriver.class);

    public static void main(String[] args) throws Exception {

        String inputPath = "data\\wordcount\\wordcount.txt";
        String outputPath = "output\\wordcount\\";

        //  获取job对象信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //删除输出文件
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }
        // 设置加载jar位置
        job.setJarByClass(WordcountDriver.class);

        // 设置mapper和reducer的class类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 设置输出mapper的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终数据输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 处理小文件
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        // 设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
