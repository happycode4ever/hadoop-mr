package com.jj.worcount_combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountCombinerDriver {
    public static void main(String[] args) throws Exception{
        //获取job实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar路径
        job.setJarByClass(WordCountCombinerDriver.class);
        //设置mapper，reducer类
        job.setMapperClass(WordCountCombinerMap.class);
        job.setReducerClass(WordCountCombinerReduce.class);
        //设置mapper输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置combiner
        job.setCombinerClass(WordCountCombiner.class);

        //**设置读取文件切片的类，默认TextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMinInputSplitSize(job,2097152);//2M
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);//4M


        //设置reducer输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入输出目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result);


    }
}
