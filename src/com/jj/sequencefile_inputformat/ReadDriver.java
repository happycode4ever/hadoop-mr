package com.jj.sequencefile_inputformat;

import com.jj.util.FileInputOutputFormatUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class ReadDriver {
    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(ReadDriver.class);
        job.setMapperClass(ReadMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputOutputFormatUtil.initInputOutput(job,"H:\\bigdata-dev\\tools\\hadoop-data\\mr-custominputformat\\output\\part-r-00000","H:\\bigdata-dev\\tools\\hadoop-data\\mr-custominputformat\\output2");
        job.waitForCompletion(true);
    }
}
