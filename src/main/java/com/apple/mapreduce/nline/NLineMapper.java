package com.apple.mapreduce.nline;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: NLineMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 16:01
 * @Version 1.1.0
 **/
public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text k = new Text();

    private LongWritable v = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] splited = line.split(" ");

        for (int i = 0; i < splited.length; i++) {
            k.set(splited[i]);
            context.write(k, v);
        }
    }
}
