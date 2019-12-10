package com.lwx.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {


    public static class SplitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //获取一行
            String line = value.toString();

            //切割
            String[] words = line.split(" ");

            //输出
            for (String word : words) {
                k.set(word);
                context.write(k, v);
            }

        }
    }


    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        int sum;
        IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            //计数
            for (IntWritable count : values) {
                sum += count.get();
            }

            v.set(sum);
            context.write(key, v);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1获取配置信息及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //2设置jar加载路径
        job.setJarByClass(WordCount.class);
        //3设置map和reduce类
        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(SumReducer.class);
        //4设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5设置最终k v输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //6设置输入和输出路径

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }


}
