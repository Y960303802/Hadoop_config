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

//数据清洗
public class PhoneCleanDataJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PhoneCleanDataJob(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        //创建job作业
        Job job = Job.getInstance(getConf(),"phoneCleanDataJob");
        job.setJarByClass(PhoneCleanDataJob.class);

        //设置InputFormate
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/phonedata/data.log"));

        //设置map
        job.setMapperClass(PhoneCleanDataJob.PhoneCleanDataMap.class);
        //下面修改了这里一定也要改 相对应
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //shuffle  无须设置 自动完成
        job.setNumReduceTasks(0); //没有reducejieduan

       //设置Output Format
        job.setOutputFormatClass(TextOutputFormat.class);
        //结果输出的位置
        Path res = new Path("/phonedata/res03");
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
    public static class PhoneCleanDataMap extends Mapper<LongWritable, Text,Text,Text> {

        @Override //参数1:行首字母偏移量  参数2:当前row数据 参数3:map输出上下文
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           //分割当前行的数据
            String[] values = value.toString().split("\t");
            String phoneCode=values[1];
            String ups=values[6];
            String downs=values[7];
            //手机号 Text
            context.write(new Text(phoneCode),new Text(ups+"\t"+downs));
        }
    }


}