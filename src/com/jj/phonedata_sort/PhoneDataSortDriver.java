package com.jj.phonedata_sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PhoneDataSortDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PhoneDataSortDriver.class);
        job.setMapperClass(PhoneDataSortMapper.class);
        job.setReducerClass(PhoneDataSortReducer.class);
        job.setMapOutputKeyClass(PhoneDataBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneDataBean.class);
        //设置分区修改按省输出文件
        job.setPartitionerClass(PhoneDataSortPartitioner.class);
        job.setNumReduceTasks(5);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.out.println(res);
    }
}
