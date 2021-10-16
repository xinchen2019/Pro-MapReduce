package com.apple.mapreduce.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: OrderReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-10 17:17
 * @Version 1.1.0
 **/
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    //0000001 222.8
    //0000002 25.8
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values,
                          Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());
    }
}
