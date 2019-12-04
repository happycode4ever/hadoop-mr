package com.jj.order_secondsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,OrderBean, NullWritable> {
    private OrderBean bean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        int orderId = Integer.parseInt(fields[0]);
        String goodId = fields[1];
        double price = Double.parseDouble(fields[2]);
        bean.setOrderId(orderId);
        bean.setGoodId(goodId);
        bean.setPrice(price);
        context.write(bean,NullWritable.get());
    }
}
