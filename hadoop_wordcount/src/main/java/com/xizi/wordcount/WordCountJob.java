package com.xizi.wordcount;


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

public class WordCountJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        //执行job作业任务类的对象是谁
        ToolRunner.run(new WordCountJob(), args);
    }



    @Override
    public int run(String[] strings) throws Exception {
        //创建job作业对象
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCountJob.class);

        //设置Input Format
        job.setInputFormatClass(TextInputFormat.class);
        //支持设置一个hdfs系统目录job作业执行会将这个目录中所有的文件参与计算
        TextInputFormat.addInputPath(job, new Path("/wordcount/aa.log"));

        //设置map阶段
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Shuffle 阶段 默认

        //设置reduce 阶段
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置 Output Formate
        job.setOutputFormatClass(TextOutputFormat.class);
        FileSystem fileSystem = FileSystem.get(getConf());

        Path res = new Path("/wordcount/result");
        if(fileSystem.exists(res)) {
            fileSystem.delete(res,true);
        }
        //保证outputFormat 输出的结果目录一定不存在
        TextOutputFormat.setOutputPath(job,res);
        //提交job作业
//        job.submit();
        boolean status = job.waitForCompletion(true);
        System.out.println("word count status="+status);
        return 0;
    }

    //map阶段
    // 泛型1：keyin inputFormat 中输出key类型  泛型2 input format 输出value类型
    //泛型3 keyout map阶段输出key 类型  泛型4 valueout map阶段输出value的类型
    public static class WordCountMap extends Mapper<LongWritable, Text,Text,IntWritable>{
        private Logger logger=Logger.getLogger(WordCountMap.class);

        @Override //参数1 这行首行偏移量 参数2 这行值  inputFormat输出一次调用一次这个方法
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("key:"+key);
            System.out.println("value"+value);
            logger.info("map=====> keyin"+key);
            logger.info("map=====> valuein:"+value);

            //对一行数据进行拆分
            String[] words = value.toString().split(" ");
            for (String word:words) {
                context.write(new Text(word), new IntWritable(1));
                logger.info("map====> keyout:"+word);
                logger.info("map====> valueout:"+new IntWritable(1));
            }
        }
    }

    //reduce阶段  汇总阶段
    public static class  WordCountReduce  extends Reducer<Text,IntWritable,Text,IntWritable>{

        private Logger logger=Logger.getLogger(WordCountReduce.class);
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            logger.info("reduce==============> keyin:"+key);
            logger.info("reduce==============> valuein:"+values);
            //可以map阶段输出的key
            int sum=0;
            for(IntWritable value:values){
                    sum+=value.get();
            }
            //输出结果
            context.write(key,new IntWritable(sum));
        }
    }


}
