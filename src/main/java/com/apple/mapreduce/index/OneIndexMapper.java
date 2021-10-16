package com.apple.mapreduce.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: OrderIndexMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 09:52
 * @Version 1.1.0
 **/
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    String fileName;
    Text k = new Text();
    IntWritable v = new IntWritable();

    /**
     * setup 在map函数之前执行一些准备工作，如作业的一些配置信息等
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        //获取文件名称
        FileSplit split = (FileSplit) context.getInputSplit();

        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] fields = line.split(" ");

        for (String word : fields) {
            k.set(word + "--" + fileName);
            v.set(1);
            context.write(k, v);
        }
    }
}
