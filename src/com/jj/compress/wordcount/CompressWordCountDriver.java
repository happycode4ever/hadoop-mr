package com.jj.compress.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompressWordCountDriver {
    public static void main(String[] args) throws Exception{
        //获取job实例
        Configuration conf = new Configuration();

//        conf.set("mapreduce.map.output.compress","true");
//        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        //设置map端压缩开启与编码方式
        conf.setBoolean("mapreduce.map.output.compress",true);
        conf.setClass("mapreduce.map.output.compress.codec",BZip2Codec.class, CompressionCodec.class);
        Job job = Job.getInstance(conf);
        //设置jar路径
        job.setJarByClass(CompressWordCountDriver.class);
        //设置mapper，reducer类
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);
        //设置mapper输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //**设置读取文件切片的类，默认TextInputFormat
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMinInputSplitSize(job,2097152);//2M
//        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);//4M


        //设置reducer输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入输出目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //设置reduce输出文件压缩开启与编码方式
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job,GzipCodec.class);
        //提交
        boolean result = job.waitForCompletion(true);
        System.out.println(result);


    }
}
