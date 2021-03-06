package com.apple.mapreduce.table;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.commons.beanutils.BeanUtils.copyProperties;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TableReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-26 16:38
 * @Version 1.1.0
 **/
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context)
            throws IOException, InterruptedException {
        //1 准备存储订单的集合
        ArrayList<TableBean> orderBeans = new ArrayList<TableBean>();
        //2 准备bean对象
        TableBean pdBean = new TableBean();

        for (TableBean bean : values) {
            if ("0".equals(bean.getFlag())) {
                //拷贝传递过来的每条订单数据到集合中
                TableBean orderBean = new TableBean();
                try {
                    copyProperties(orderBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                orderBeans.add(orderBean);
            } else {
                //产品表
                try {
                    copyProperties(pdBean, bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        //3 表的拼接
        for (TableBean bean : orderBeans) {
            bean.setPname(pdBean.getPname());
            //4 数据写出去
            context.write(bean, NullWritable.get());
        }
    }
}
