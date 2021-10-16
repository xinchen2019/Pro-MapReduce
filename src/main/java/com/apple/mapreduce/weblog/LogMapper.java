package com.apple.mapreduce.weblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: LogMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 00:39
 * @Version 1.1.0
 **/
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取1行数据
        String line = value.toString();

        //解析日志
        boolean result = parseLog(line, context);

        //日志不合法退出
        if (!result) {
            return;
        }

        //设置key
        k.set(line);

        //写出数据
        context.write(k, NullWritable.get());

    }

    //解析日志
    private boolean parseLog(String line, Context context) {

        //截取
        String[] fields = line.split(" ");

        //日志长度大于11位合法
        if (fields.length > 11) {
            //系统计数器
            context.getCounter("map", "true").increment(1);
            return true;
        } else {
            context.getCounter("map", "false").increment(1);
            return false;
        }

    }
}
