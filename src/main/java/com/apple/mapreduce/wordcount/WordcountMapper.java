package com.apple.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: WordcountMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-27 11:45
 * @Version 1.1.0
 **/
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static Logger logger = LoggerFactory.getLogger(WordcountMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 获取这一行数据
        String line = value.toString();

        // 获取每一个单词
        String[] words = line.split(" ");

        for (String word : words) {
            // 输出每一个单词
            context.write(new Text(word), new IntWritable(1));
        }
    }
}





