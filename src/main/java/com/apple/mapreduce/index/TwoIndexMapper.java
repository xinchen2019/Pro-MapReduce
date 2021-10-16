package com.apple.mapreduce.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TwoIndex
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 10:15
 * @Version 1.1.0
 **/
public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text k = new Text();

    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取一行数据
        String line = value.toString();
        //用“--”切割
        String[] fields = line.split("--");

        k.set(fields[0]);
        v.set(fields[1]);

        //输出数据
        context.write(k, v);
    }
}
