package com.apple.mapreduce.flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: FlowCountSortMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-01 11:22
 * @Version 1.1.0
 **/
public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean bean = new FlowBean();

    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phoneNumber = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        bean.set(upFlow, downFlow);
        v.set(phoneNumber);

        context.write(bean, v);
    }
}
