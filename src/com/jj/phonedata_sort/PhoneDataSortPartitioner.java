package com.jj.phonedata_sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class PhoneDataSortPartitioner extends Partitioner<PhoneDataBean,Text> {
    @Override
    public int getPartition(PhoneDataBean phoneDataBean, Text text, int numPartitions) {
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
