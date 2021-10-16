package hadoopone;

import org.apache.hadoop.net.DNSToSwitchMapping;

import java.util.List;

/**
 * @Program: HadoopDemo
 * @ClassName: AutoRack
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-27 20:08
 * @Version 1.1.0
 **/
public class AutoRack implements DNSToSwitchMapping {
    public List<String> resolve(List<String> list) {
        for (int i = 0; i < list.size(); i++) {
            System.out.println(i + list.get(i));
        }
        return list;
    }

    public void reloadCachedMappings() {

    }

    public void reloadCachedMappings(List<String> list) {

    }
}
