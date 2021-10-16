package com.apple.mapreduce.weblog;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: LogDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 23:14
 * @Version 1.1.0
 **/
public class LogDriver {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{
                "data\\weblog\\web.log",
                "output\\weblog"
        };

        //获取Job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //加载jar包
        job.setJarByClass(LogDriver.class);

        //关联map
        job.setMapperClass(LogMapper.class);

        //设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置reducetask个数为0
        job.setNumReduceTasks(0);

        //设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交
        job.waitForCompletion(true);

    }
}
