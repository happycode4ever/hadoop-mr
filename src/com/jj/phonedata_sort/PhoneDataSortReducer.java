package com.jj.phonedata_sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneDataSortReducer extends Reducer<PhoneDataBean,Text, Text,PhoneDataBean> {
    @Override
    protected void reduce(PhoneDataBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text phoneText = values.iterator().next();
        context.write(phoneText,key);
    }
}
