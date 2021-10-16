package com.apple.mapreduce.flowsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: FlowCountSortReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-01 11:40
 * @Version 1.1.0
 **/
public class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        //循环输出，避免总流量相同情况
        for (Text text : values) {
            context.write(text, key);
        }
    }
}
