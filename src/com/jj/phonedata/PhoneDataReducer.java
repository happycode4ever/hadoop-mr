package com.jj.phonedata;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneDataReducer extends Reducer<Text,PhoneDataBean,Text,PhoneDataBean> {
    @Override
    protected void reduce(Text key, Iterable<PhoneDataBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0L;
        long sumDownFlow = 0L;
        for(PhoneDataBean bean : values){
            sumUpFlow += bean.getUpFlow();
            sumDownFlow += bean.getDownFlow();
        }
        context.write(key,new PhoneDataBean(sumUpFlow,sumDownFlow));
    }
}
