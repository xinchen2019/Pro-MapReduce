package hadoopone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * @Program: HadoopDemo
 * @ClassName: HdfsClient
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-27 19:37
 * @Version 1.1.0
 **/
public class HDFSClient {

    private static Logger logger = LoggerFactory.getLogger(HDFSClient.class);

    public static void main(String[] args) throws Exception {

        //  获取配置信息
        Configuration configuration = new Configuration();
        //  configuration.set("fs.defaultFS", "hdfs://master:8020");

        //  获取文件系统
        //  FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), configuration, "ubuntu");

        if (!fs.exists(new Path("/user/xiyou1.txt"))) {
            fs.copyFromLocalFile(new Path("C:\\Users\\13128\\Desktop\\1.txt"), new Path("/user/xiyou1.txt"));
            fs.setReplication(new Path("/user/xiyou1.txt"), (short) 1);
        }
        //  拷贝本地数据到集群

        //fs.setReplication(new Path("/user/xiyou1.txt"),(short)1);

        //  关闭fs
        fs.close();
    }
}
