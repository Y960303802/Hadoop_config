package com.xizi.phonedata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
//这个是对统计出来的流量进行排序
//topn 进行选择前n 条数据
public class PhoneDataSortJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PhoneDataSortJob(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        //创建job作业
        Job job = Job.getInstance(getConf(), "phonedata-log");
        job.setJarByClass(PhoneDataSortJob.class);

        //设置InputFormate
        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job,new Path("/phonedata/res/part-r-00000"));

        //设置map
        job.setMapperClass(PhoneDataSortMap.class);


        //输出key
        job.setMapOutputKeyClass(PhoneWritable.class);
        //输出value
        job.setMapOutputValueClass(Text.class);

        //shuffle  无须设置 自动完成

        //设置reduce
        job.setReducerClass(PhoneDataSortReduce.class);
        //输出key
        job.setOutputKeyClass(Text.class);
        //输出value
        job.setOutputValueClass(PhoneWritable.class);

        //设置Output Format
        job.setOutputFormatClass(TextOutputFormat.class);
        //Hdfs中的文件

        Path res = new Path("/phonedata/res02");
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


    //map阶段  key PhoneWritable进行排序
    public static class PhoneDataSortMap extends Mapper<LongWritable, Text,PhoneWritable,Text> {

        @Override //参数1:行首字母偏移量  参数2:当前row数据 参数3:map输出上下文
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            String phonecode=values[0];
            PhoneWritable phoneWritable = new PhoneWritable(Integer.valueOf(values[1]), Integer.valueOf(values[2]),Integer.valueOf(values[3]));
            context.write(phoneWritable,new Text(phonecode));
        }
    }
    //reduce
    // reduce 的输出 输入改成PhoneWritable
    public static class PhoneDataSortReduce extends Reducer<PhoneWritable,Text,Text,PhoneWritable> {
        int sum=0;
        @Override
        protected void reduce(PhoneWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(sum<3){
                String phonecode="";
                for(Text value :values){
                    phonecode=value.toString();
                }
                context.write(new Text(phonecode),key );
                sum++;
            }
        }
    }

}