package com.jj.phonedata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PhoneDataDriver {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PhoneDataDriver.class);
        job.setMapperClass(PhoneDataMapper.class);
        job.setReducerClass(PhoneDataReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneDataBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneDataBean.class);
        //设置分区
        job.setPartitionerClass(PhoneDataPartitioner.class);
        //reduce任务数1输出1个文件，等于分区数才是分区文件，小于分区数抛IO异常，多于分区数会多出空文件
        job.setNumReduceTasks(1);
        //输入输出
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        boolean res = job.waitForCompletion(true);
        System.out.println(res);


    }
}
