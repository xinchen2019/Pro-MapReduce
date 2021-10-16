package com.apple.mapreduce.compress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TestCompress
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 12:13
 * @Version 1.1.0
 **/
public class TestCompress {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException {

        args = new String[]{
                "data\\compress\\hello.txt",
                "org.apache.hadoop.io.compress.BZip2Codec"
        };
        //ccompress(args[0], args[1]);

        ddecompress("data\\compress\\hello.txt.bz2");
    }

    //压缩
    private static void ccompress(String fileName, String method)
            throws IOException, ClassNotFoundException {

        // 获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));

        Class codecClass = Class.forName(method);

        CompressionCodec codec = (CompressionCodec) ReflectionUtils
                .newInstance(codecClass, new Configuration());

        //获取输出流
        FileOutputStream fos = new FileOutputStream(
                new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //流的对拷
        IOUtils.copyBytes(fis, cos, 1024 * 1024 * 5, false);

        //关闭资源
        fis.close();
        cos.close();
        fos.close();
    }

    //解压缩
    private static void ddecompress(String fileName)
            throws IOException {

        //检验是否能解压缩
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

        CompressionCodec codec = factory.getCodec(new Path(fileName));

        if (codec == null) {
            System.out.println("cannot find codec for file" + fileName);
            return;
        }

        //获取输入流
        CompressionInputStream cis = codec.createInputStream(
                new FileInputStream(new File(fileName)));

        //获取输出流
        FileOutputStream fos = new FileOutputStream(
                new File(fileName + ".decoded"));

        //流的对拷
        IOUtils.copyBytes(cis, fos, 1024 * 1024 * 5, false);

        //关闭资源
        cis.close();
        fos.close();
    }
}
