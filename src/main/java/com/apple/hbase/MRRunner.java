package com.apple.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: MRRunner
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-05 12:22
 * @Version 1.1.0
 * 参考链接：https://blog.csdn.net/weixin_43230682/article/details/107534039
 * /
 * /**
 * create 'apple:table777','contact','information'
 * create 'apple:table777_mr','contact','information'
 * put 'apple:table777', 'stu-001', 'contact:email', '123@163.com'
 * put 'apple:table777', 'stu-001', 'contact:phone', '18351903812'
 * put 'apple:table777', 'stu-001', 'information:age', '27'
 * put 'apple:table777', 'stu-001', 'information:gender', 'M'
 * put 'apple:table777', 'stu-001', 'information:name', 'Ace'
 */
public class MRRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {


        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        int status = ToolRunner.run(configuration, new MRRunner(), args);

        System.exit(status);

    }

    public int run(String[] args) throws Exception {

        String sourceTable = "apple:table777";
        String targetTable = "apple:table777_mr";

        //获取configuration对象
        Configuration conf = this.getConf();

        //创建Job任务
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());

        job.setJarByClass(MRRunner.class);
        //配置job
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        /**
         * Use this before submitting a TableMap job. It will appropriately set up
         * the job.
         *
         * @param table  The table name to read from.
         * @param scan  The scan instance with the columns, time range etc.
         * @param mapper  The mapper class to use.
         * @param outputKeyClass  The class of the output key.
         * @param outputValueClass  The class of the output value.
         * @param job  The current job to adjust.  Make sure the passed job is
         * carrying all necessary HBase configuration.
         * @throws IOException When setting up the details fails.
         */
        TableMapReduceUtil.initTableMapperJob(
                sourceTable,
                scan,
                ReadStudentMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job);

        TableMapReduceUtil.initTableReducerJob(
                targetTable,
                WriteStudentReducer.class,
                job
        );

        //设置Reduce数量，最少1个
        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
        if (!isSuccess) {
            throw new IOException("Job running with errpr");
        }
        return isSuccess ? 0 : 1;
    }
}
