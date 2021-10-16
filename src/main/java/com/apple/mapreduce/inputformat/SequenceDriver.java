package com.apple.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: SequenceDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-11 16:17
 * @Version 1.1.0
 **/

/**
 * 1）需求 无论hdfs还是mapreduce，对于小文件都有损效率，实践中，
 * 又难免面临处理大量小文件的场景，此时，就需要有相应解决方案。
 * 将多个小文件合并成一个文件SequenceFile，SequenceFile里面存储着多个文件，
 * 存储的形式为文件路径+名称为key，文件内容为value。
 */
public class SequenceDriver {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{
                "data\\inputformat",
                "output\\inputformat"
        };
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SequenceDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 设置输入的inputFormat
        job.setInputFormatClass(WholeFileInputformat.class);
        // 设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
