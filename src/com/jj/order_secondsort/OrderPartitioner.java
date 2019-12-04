package com.jj.order_secondsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable value, int numPartitions) {
        return (orderBean.getOrderId()&Integer.MAX_VALUE)%numPartitions;
    }
}
