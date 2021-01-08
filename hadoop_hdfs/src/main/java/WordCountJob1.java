

import org.apache.hadoop.conf.Configured;
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

public class WordCountJob1 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        //执行job作业任务类的对象是谁
        ToolRunner.run(new WordCountJob1(), args);
    }



    @Override
    public int run(String[] strings) throws Exception {
        //创建job作业对象
        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCountJob1.class);

        //设置Input Format
        job.setInputFormatClass(TextInputFormat.class);
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
        //保证outputFormat 输出的结果目录一定不存在
        TextOutputFormat.setOutputPath(job,new Path("/wordcount/result/") );
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
        @Override //参数1 这行首行偏移量 参数2 这行值  inputFormat输出一次调用一次这个方法
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //对一行数据进行拆分
            String[] words = value.toString().split(" ");
            for (String word:words)
                context.write(new Text(word), new IntWritable(1));
        }
    }

    //reduce阶段  汇总阶段
    public static class  WordCountReduce  extends Reducer<Text,IntWritable,Text,IntWritable>{
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


}
