package com.apple.mapreduce.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @Program: Pro-MapReduce
 * @ClassName: OrderPatitioner
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-10 16:49
 * @Version 1.1.0
 **/
public class OrderPatitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean key, NullWritable nullWritable, int numPartitions) {
        return (key.getOrderId() & Integer.MAX_VALUE) % numPartitions;
    }
}
