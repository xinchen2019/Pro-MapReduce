package com.apple.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: FlowMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-28 00:56
 * @Version 1.1.0
 **/
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    FlowBean bean = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //  获取一行数据
        String line = value.toString();

        //  截取字段
        String[] fields = line.split("\t");

        //  封装bean对象以及获取电话号
        String phoneNum = fields[1];

        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        bean.set(upFlow, downFlow);
        k.set(phoneNum);
        //  写出去
        context.write(k, bean);
    }
}
