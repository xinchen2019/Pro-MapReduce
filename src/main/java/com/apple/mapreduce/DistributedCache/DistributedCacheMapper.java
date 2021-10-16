package com.apple.mapreduce.DistributedCache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TableMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-26 16:26
 * @Version 1.1.0
 **/
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<String, String>();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //获取缓存的文件
        URI[] url = context.getCacheFiles();
        URI pduri = null;
        for (URI uri : url) {
            if (uri.toString().indexOf("pd.txt") >= 0) {
                pduri = uri;
            }
        }
        System.out.println("pduri:" + pduri);
        //获取缓存的文件
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(
                                new File(pduri.getPath())), "UTF-8"));
        String line;

        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //切割
            String[] fields = line.split("\t");
            //缓存数据到集合
            pdMap.put(fields[0], fields[1]);
        }
        //关流
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取一行
        String line = value.toString();

        //截取
        String[] fields = line.split("\t");

        //获取产品id
        String pId = fields[1];

        //获取商品名单
        String pdName = pdMap.get(pId);

        //拼接
        k.set(line + "\t" + pdName);

        //写出
        context.write(k, NullWritable.get());

    }
}
