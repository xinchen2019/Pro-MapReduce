package com.apple.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: WordcountReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-27 11:46
 * @Version 1.1.0
 **/
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private static Logger logger = LoggerFactory.getLogger(WordcountReducer.class);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        //  统计所有单词个数
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        // 输出所有单词个数
        context.write(key, new IntWritable(count));
    }
}
