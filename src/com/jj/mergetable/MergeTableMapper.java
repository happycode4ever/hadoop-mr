package com.jj.mergetable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MergeTableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private Text k = new Text();
    private TableBean bean = new TableBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        if(fileName.startsWith("order")){
            String orderId = fields[0];
            String pId = fields[1];
            int amount = Integer.parseInt(fields[2]);
            bean.setOrderId(orderId);
            bean.setpId(pId);
            bean.setAmount(amount);
            bean.setType("order");
            k.set(pId);
        }
        if(fileName.startsWith("pd")){
            String pId = fields[0];
            String productName = fields[1];
            bean.setpId(pId);
            bean.setProductName(productName);
            bean.setType("pd");
            k.set(pId);
        }
        context.write(k,bean);
    }
}
