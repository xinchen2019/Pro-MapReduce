package com.apple.mapreduce.compress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: WordCountDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 13:30
 * @Version 1.1.0
 **/
public class WordCountDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{
                "data\\compress\\hello.txt",
                "output\\compress"
        };

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        //开启map端输出压缩
        conf.setBoolean("mapreduce.map.output.compress", true);

        //设置map端输出压缩方式
        conf.setClass("mapreduce.map.output.compress.codec",
                BZip2Codec.class,
                CompressionCodec.class);

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(com.apple.mapreduce.compress.WordCountMapper.class);
        job.setReducerClass(com.apple.mapreduce.compress.WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置reduce端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);

        //设置压缩方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        //FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }
}
