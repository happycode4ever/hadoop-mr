package com.jj.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class CompressTest {
    public static void main(String[] args) throws Exception{
//        compress("H:\\bigdata-dev\\tools\\hadoop-data\\compress\\server.log","org.apache.hadoop.io.compress.BZip2Codec");
        decompress("H:\\bigdata-dev\\tools\\hadoop-data\\compress\\server.log.bz2");
    }
    public static void compress(String sourcePath, String codecClazz) throws Exception{
        Configuration conf = new Configuration();
        FileInputStream fis = new FileInputStream(sourcePath);
        //根据压缩方式获取类
        Class<?> codecClass = Class.forName(codecClazz);
        //反射构建
        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        //获取压缩方式添加后缀
        FileOutputStream fos = new FileOutputStream(sourcePath+compressionCodec.getDefaultExtension());
        CompressionOutputStream cos = compressionCodec.createOutputStream(fos);
        IOUtils.copyBytes(fis,cos,1024*1024*5,true);
    }

    public static void decompress(String sourcePath) throws Exception{
        Configuration conf = new Configuration();
        //工厂对象获取压缩编码
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
        CompressionCodec codec = compressionCodecFactory.getCodec(new Path(sourcePath));
        if(codec==null){
            System.out.println("not support codec for file:"+sourcePath);
            return;
        }
        File sourceFile = new File(sourcePath);
        FileInputStream fis = new FileInputStream(sourceFile);
        String destPath = sourceFile.getParent()+"/decompress/"+sourceFile.getName().substring(0,sourceFile.getName().lastIndexOf(codec.getDefaultExtension()));
        FileOutputStream fos = new FileOutputStream(destPath);
        CompressionInputStream cis = codec.createInputStream(fis);
        IOUtils.copyBytes(cis,fos,1024*1024*5,true);
    }

    @Test
    public void test(){
        File file = new File("H:\\bigdata-dev\\tools\\hadoop-data\\compress\\server.log");
        System.out.println(file.getName());
    }
}
