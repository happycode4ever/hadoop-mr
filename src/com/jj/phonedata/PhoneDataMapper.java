package com.jj.phonedata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneDataMapper extends Mapper<LongWritable, Text,Text,PhoneDataBean> {

//    1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200
//    1363157995052 	13826544101	5C-0E-8B-C7-F1-E0:CMCC	120.197.40.4			4	0	264	0	200

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        //拆分信息
        String phone = fields[1];
        long upFlow = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);
        PhoneDataBean bean = new PhoneDataBean(upFlow, downFlow);
        //组装bean给到Reduce
        context.write(new Text(phone),bean);
    }
}
