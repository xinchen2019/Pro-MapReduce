package com.apple.mapreduce.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @Program: Pro-MapReduce
 * @ClassName: OrderMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-10 17:14
 * @Version 1.1.0
 **/
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean bean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /**
         *  1.读取数据
         */
        String line = value.toString();
        /**
         *  2.切割数据
         */
        String[] fields = line.split("\t");
        //order_000002  pdt_03  522.8
        /**
         *  3.封装bean对象
         */
        bean.setOrderId(Integer.parseInt(fields[0]));
        bean.setPrice(Double.parseDouble(fields[2]));

        context.write(bean, NullWritable.get());
    }
}
