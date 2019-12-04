package com.jj.phonedata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//分区是reducer之前也就是mapper输出
public class PhoneDataPartitioner extends Partitioner<Text,PhoneDataBean> {
    @Override
    public int getPartition(Text text, PhoneDataBean phoneDataBean, int numPartitions) {
        String zone = text.toString().substring(0, 3);
        if("136".equals(zone)){
            return 0;
        }else if("137".equals(zone)){
            return 1;
        }else if("138".equals(zone)){
            return 2;
        }else if("139".equals(zone)){
            return 3;
        }
        return 4;
    }
}
