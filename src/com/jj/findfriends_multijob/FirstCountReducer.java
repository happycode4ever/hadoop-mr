package com.jj.findfriends_multijob;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeSet;

public class FirstCountReducer extends Reducer<Text,Text,Text, NullWritable> {
    private Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TreeSet<String> ts = new TreeSet<>();
        for(Text t : values){
            ts.add(t.toString());
        }
        String owners = ts.toString().replace("[","").replace("]","").replace(" ","");
        v.set(key.toString()+":"+owners);
        context.write(v,NullWritable.get());
    }
}
