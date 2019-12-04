package com.jj.phonedata_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//排序需要放在key
public class PhoneDataSortMapper extends Mapper<LongWritable,Text,PhoneDataBean,Text> {
    private Text value = new Text();
    private PhoneDataBean bean = new PhoneDataBean();
//        13480253104	180	180	360
//        13502468823	7335	110349	117684
//        13560436666	3597	25635	29232
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String phone = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        bean.setAllFlow(upFlow,downFlow);
        this.value.set(phone);
        context.write(bean, this.value);
    }

}
