package com.apple.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: ReadStudentMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-05 12:33
 * @Version 1.1.0
 **/
public class ReadStudentMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {

        Put put = new Put(key.get());

        for (Cell cell : value.rawCells()) {
            if ("information".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                }
            } else if ("contact".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                if ("phone".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                }
            }
        }
        context.write(key, put);
    }
}
