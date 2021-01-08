package com.xizi.phonedata;

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


import java.io.IOException;

//统计手机流量
public class PhoneDataJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PhoneDataJob(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        //创建job作业
        Job job = Job.getInstance(getConf(), "phonedata-log");
        job.setJarByClass(PhoneDataJob.class);

        //设置InputFormate
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/phonedata/data.log"));

        //设置map
        job.setMapperClass(PhoneDataJob.PhoneDataMap.class);
        //下面修改了这里一定也要改 相对应
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneWritable.class);

        //shuffle  无须设置 自动完成

        //设置reduce
        job.setReducerClass(PhoneDataJob.PhoneDataReduce.class);
        //下面修改了这里一定也要改 相对应
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneWritable.class);

        //设置Output Format
        job.setOutputFormatClass(TextOutputFormat.class);
        //结果输出的位置
        Path res = new Path("/phonedata/res");
        FileSystem fileSystem = FileSystem.get(getConf());
        //存在就删除
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
    public static class PhoneDataMap extends Mapper<LongWritable, Text,Text,PhoneWritable> {

        @Override //参数1:行首字母偏移量  参数2:当前row数据 参数3:map输出上下文
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           //分割当前行的数据
            String[] values = value.toString().split("\t");
            String phoneCode=values[1];
            String ups=values[6];
            String downs=values[7];
            PhoneWritable phoneWritable = new PhoneWritable();
            phoneWritable.setUploads(Integer.valueOf(ups));
            phoneWritable.setDownloads(Integer.valueOf(downs));
            phoneWritable.setTotals(0);
            //手机号 Text
            //up down total phoneWritable
            context.write(new Text(phoneCode),phoneWritable);
        }
    }
    //reduce
    // reduce 的输出 输入改成PhoneWritable
    public static class PhoneDataReduce extends Reducer<Text,PhoneWritable,Text,PhoneWritable> {
        @Override //参数1:map的key  参数2:相当key的数组   参数3:Reduce输出的上下文
        protected void reduce(Text key, Iterable<PhoneWritable> values, Context context) throws IOException, InterruptedException {
            int uploadData = 0; //保存上传流量
            int downData = 0;   //保存下载流量
            for (PhoneWritable value : values) {

                uploadData+=value.getUploads();
                downData+= value.getDownloads();
            }
                PhoneWritable phoneWritable = new PhoneWritable(uploadData, downData, (uploadData + downData));
            //输出
            //key 手机号
            context.write(key, phoneWritable);
        }
    }

}