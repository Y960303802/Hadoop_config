package com.xizi.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//reduce阶段  汇总阶段
public  class  WordCountReduce  extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //可以map阶段输出的key
        int sum=0;
        for(IntWritable value:values){
            sum+=value.get();
        }
        //输出结果
        context.write(key,new IntWritable(sum));
    }
}
