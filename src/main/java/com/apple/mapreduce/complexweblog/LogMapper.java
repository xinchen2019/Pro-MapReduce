package com.apple.mapreduce.complexweblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: LogMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 00:02
 * @Version 1.1.0
 **/
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取一行
        String line = value.toString();

        //解析日志是否合法
        LogBean bean = pressLog(line);

        if (!bean.isValid()) {
            return;
        }

        k.set(bean.toString());

        context.write(k, NullWritable.get());
    }

    //解析日志
    private LogBean pressLog(String line) {

        LogBean logBean = new LogBean();

        //截取
        String[] fields = line.split(" ");

        if (fields.length > 11) {
            //封装数据
            logBean.setRemote_addr(fields[0]);
            logBean.setRemote_user(fields[1]);
            logBean.setTime_local(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBody_bytes_sent(fields[9]);
            logBean.setHttp_referer(fields[10]);

            if (fields.length > 12) {
                logBean.setHttp_user_agent(fields[11] + "" + fields[12]);
            } else {
                logBean.setHttp_user_agent(fields[11]);
            }

            //大于400，HTTP错误
            if (Integer.parseInt(logBean.getStatus()) >= 400) {
                logBean.setValid(false);
            }
        } else {
            logBean.setValid(false);
        }
        return logBean;
    }
}
