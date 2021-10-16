package com.apple.mapreduce.complexweblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: LogDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 09:26
 * @Version 1.1.0
 **/
public class LogDriver {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{
                "data\\weblog\\web.log",
                "output\\complexweblog"
        };

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        //获取job信息
        Job job = Job.getInstance(conf);

        //加载jar包
        job.setJarByClass(LogDriver.class);

        //关联map
        job.setMapperClass(LogMapper.class);

        //设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交
        job.waitForCompletion(true);
    }
}
