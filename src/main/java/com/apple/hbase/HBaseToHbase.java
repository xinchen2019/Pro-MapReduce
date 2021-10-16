package com.apple.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Iterator;


/**
 * @Program: Hadoop
 * @ClassName: HBaseToHbase
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-09-25 12:34
 * @Version 1.1.0
 * 参考链接 https://blog.csdn.net/zjttlance/article/details/83820349
 **/
public class HBaseToHbase {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        //源表
        String sourceTable = "apple:hello";
        //目标表
        String targetTable = "apple:mytb2";

        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        prepareTB2(targetTable, conf);

        Job job = Job.getInstance(conf);

        job.setJarByClass(HBaseToHbase.class);

        job.setJobName("MRToHbase");

        Scan scan = new Scan();

        scan.setCaching(500);

        scan.setCacheBlocks(false);


        /**
         * @param table  The table name to read from.
         * @param scan  The scan instance with the columns, time range etc.
         * @param mapper  The mapper class to use.
         * @param outputKeyClass  The class of the output key.
         * @param outputValueClass  The class of the output value.
         * @param job  The current job to adjust.  Make sure the passed job is
         * carrying all necessary HBase configuration.
         * @throws IOException When setting up the details fails.
         * 参数分别为：表名,扫描器，Mapper类，K2类型，V2类型，任务Job
         */

        TableMapReduceUtil.initTableMapperJob(
                sourceTable,
                scan,
                doMapper.class,
                Text.class,
                IntWritable.class,
                job
        );
        /**
         * @param table  The output table.
         * @param reducer  The reducer class to use.
         * @param job  The current job to adjust.
         * @throws IOException When determining the region count fails.
         * 参数分别为：表名(String)，Reducer类，任务Job
         */
        TableMapReduceUtil.initTableReducerJob(
                targetTable,
                doReducer.class,
                job
        );

        System.exit(job.waitForCompletion(true) ? 1 : 0);

    }

    private static void prepareTB2(String hbaseTableName, Configuration cfg) throws IOException {

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(hbaseTableName));

        HColumnDescriptor columnDesc = new HColumnDescriptor("mycolumnfamily");

        tableDesc.addFamily(columnDesc);

        Connection conn = ConnectionFactory.createConnection(cfg);

        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();

        System.out.println("create table:" + hbaseTableName);


        if (admin.tableExists(hbaseTableName)) {
            System.out.println("Table exists,trying drop and create!");
            admin.disableTable(TableName.valueOf(hbaseTableName));
            admin.deleteTable(TableName.valueOf(hbaseTableName));
            admin.createTable(tableDesc);
        } else {
            System.out.println("create table:" + hbaseTableName);
            admin.createTable(tableDesc);
        }
    }

    /**
     * @param <KEYOUT>   The type of the key.
     * @param <VALUEOUT> The type of the value.
     * @see org.apache.hadoop.mapreduce.Mapper
     */
    public static class doMapper extends TableMapper<Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {

            System.out.println("key: " + key);
            System.out.println("value: " + value);
            System.out.println("context: " + context);

            String rowValue = Bytes.toString(CellUtil.cloneValue(value.listCells().get(0)));

            context.write(new Text(rowValue), one);
        }
    }

    /**
     * @param <KEYIN>   The type of the input key.
     * @param <VALUEIN> The type of the input value.
     * @param <KEYOUT>  The type of the output key.
     * @see org.apache.hadoop.mapreduce.Reducer
     */
    public static class doReducer extends TableReducer<Text, IntWritable, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            System.out.println("reducer key: " + key.toString());
            int sum = 0;

            Iterator<IntWritable> it = values.iterator();
            while (it.hasNext()) {
                sum += it.next().get();
            }

            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(
                    Bytes.toBytes("mycolumnfamily"),
                    Bytes.toBytes("count")
                    , Bytes.toBytes(String.valueOf(sum)));

            context.write(NullWritable.get(), put);
        }
    }
}
