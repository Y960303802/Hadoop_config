package com.xizi.accessLogJob;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
//统计手机流量
public class AccessLogJob extends Configured implements Tool {

    private static Logger logger = Logger.getLogger(AccessLogJob.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new AccessLogJob(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        //创建job作业
        Job job = Job.getInstance(getConf(), "access-log");
        job.setJarByClass(AccessLogJob.class);

        //设置InputFormate
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/accesslog/access.log"));

        //设置map
        job.setMapperClass(AccessLogMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //shuffle  无须设置 自动完成

        //设置reduce
        job.setReducerClass(AccessLogReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置Output Format
        job.setOutputFormatClass(TextOutputFormat.class);
        Path res = new Path("/accesslog/res");
        FileSystem fileSystem = FileSystem.get(getConf());
        if(fileSystem.exists(res)) {
            fileSystem.delete(res,true);
        }
        TextOutputFormat.setOutputPath(job, res);

        //提交job作业
        boolean status = job.waitForCompletion(true);
        System.out.println("本次作业执行状态 = " + status);

        return 0;
    }


    //map阶段
    public static class AccessLogMap extends Mapper<LongWritable, Text,Text,Text>{

        @Override //参数1:行首字母偏移量  参数2:当前row数据 参数3:map输出上下文
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            //输出key 为手机号  值为: 每个手机号"上传-下载流量"格式文本
            context.write(new Text(values[1]),new Text(values[values.length-3]+"-"+values[values.length-2]));

            logger.info("手机号: "+values[1]+"  流量格式:"+values[values.length-3]+"-"+values[values.length-2]);
        }
    }
    //reduce
    public static class AccessLogReduce extends Reducer<Text,Text,Text,Text>{
        @Override //参数1:map的key  参数2:相当key的数组   参数3:Reduce输出的上下文
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int uploadData = 0; //保存上传流量
            int downData = 0;   //保存下载流量
            for (Text value : values) {
                String[] datas = value.toString().split("-");
                uploadData+= Integer.valueOf(datas[0]);
                downData+= Integer.valueOf(datas[1]);
            }
            int total = uploadData + downData;//保存总流量

            //输出
            context.write(key,new Text(" 上传流量:"+uploadData+"  下载流量:"+downData+"  总数据流量:  "+total));
            logger.info("手机号: "+key+" 上传流量:"+uploadData+"  下载流量:"+downData+"  总数据流量:  "+total);
        }
    }

}