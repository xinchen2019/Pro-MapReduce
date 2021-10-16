package com.apple.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


/**
 * @Program: Pro-MapReduce
 * @ClassName: HbaseUtils
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-06 14:11
 * @Version 1.1.0
 **/
public class HbaseUtil {

    HBaseAdmin hBaseAdmin = null;
    Connection connection = null;
    Configuration conf = null;

    private HbaseUtil() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            connection = ConnectionFactory.createConnection(conf);
            hBaseAdmin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HbaseUtil instance = null;

    public static synchronized HbaseUtil getInstance() {
        if (instance == null) {
            instance = new HbaseUtil();
        }
        return instance;
    }

    /**
     * 创建hbase表
     *
     * @param tableName
     * @param family
     * @throws IOException
     */
    public void createTable(String tableName, String[] family) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String f : family) {
            tableDescriptor.addFamily(new HColumnDescriptor(f));
        }
        hBaseAdmin.createTable(tableDescriptor);
        System.out.println("创建" + tableName + "表成功");
    }
}
