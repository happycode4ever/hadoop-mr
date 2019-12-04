package com.jj.order_secondsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        int limit = 2;
        for(NullWritable v : values){
            if(i<limit){
                System.out.println("groupkey:"+key+",value:"+v);
                context.write(key,v);
                i++;
            }
        }
    }
}
