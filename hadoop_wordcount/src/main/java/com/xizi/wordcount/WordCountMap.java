package com.xizi.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//map阶段
// 泛型1：keyin inputFormat 中输出key类型  泛型2 input format 输出value类型
//泛型3 keyout map阶段输出key 类型  泛型4 valueout map阶段输出value的类型
public  class WordCountMap extends Mapper<LongWritable, Text,Text,IntWritable> {
    @Override //参数1 这行首行偏移量 参数2 这行值  inputFormat输出一次调用一次这个方法
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //对一行数据进行拆分
        String[] words = value.toString().split(" ");
        for (String word:words)
            context.write(new Text(word), new IntWritable(1));
    }
}
