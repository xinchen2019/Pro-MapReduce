package com.apple.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Program: Pro-MapReduce
 * @ClassName: OrderGroupComparator
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-10 18:29
 * @Version 1.1.0
 **/
public class OrderGroupComparator extends WritableComparator {

    protected OrderGroupComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        int result;
        //id相同即认为同一个对象
        if (aBean.getOrderId() > bBean.getOrderId()) {
            result = 1;
        } else if (aBean.getOrderId() < bBean.getOrderId()) {
            result = -1;
        } else {
            result = 0;
        }
        return result;
    }
}
