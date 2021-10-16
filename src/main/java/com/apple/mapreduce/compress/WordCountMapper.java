package com.apple.mapreduce.compress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: WordCountMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 13:35
 * @Version 1.1.0
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取一行
        String line = value.toString();
        //切割
        String[] words = line.split(" ");

        //循环写出
        for (String word : words) {
            context.write(new Text(word), new IntWritable(1));
        }

    }
}
