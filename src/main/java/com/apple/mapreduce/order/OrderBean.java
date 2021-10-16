package com.apple.mapreduce.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: OrderBean
 * @Description: 订单详情
 * @Author Mr.Apple
 * @Create: 2021-04-10 16:39
 * @Version 1.1.0
 **/
public class OrderBean implements WritableComparable<OrderBean> {

    /**
     * 订单
     */
    private int orderId;
    /**
     * 商品价格
     */
    private Double price;

    public OrderBean() {
    }

    public OrderBean(int orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    //二次排序
    public int compareTo(OrderBean o) {

        int result;
        if (orderId > o.getOrderId()) {
            result = 1;
        } else if (orderId < o.getOrderId()) {
            result = -1;
        } else {
            //价格倒序排序
            result = price > o.getPrice() ? -1 : 1;
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeDouble(price);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readInt();
        this.price = in.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
