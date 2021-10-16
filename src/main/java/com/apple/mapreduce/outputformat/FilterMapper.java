package com.apple.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: FilterMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-11 17:25
 * @Version 1.1.0
 **/
public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        /**
         *  1 获取一行
         *  http://www.apple.com
         *  http://www.google.com
         */
        String line = value.toString();
        //2 设置k
        k.set(line);
        //3 输出
        context.write(k, NullWritable.get());
    }
}
