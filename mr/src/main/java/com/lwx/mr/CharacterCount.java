package com.lwx.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;

public class CharacterCount {

    public static class SplitMapper extends Mapper<LongWritable, Text,Text, IntWritable>{

        IntWritable v=new IntWritable(1);
        Text k=new Text();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            char[] chars = str.toCharArray();

            for(char c:chars){

                if(!Character.isWhitespace(c)){
                    String cs = String.valueOf(c);
                    k.set(cs);
                    context.write(k,v);
                }

            }

        }
    }


    public static class SumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

        IntWritable v=new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum=0;
            for(IntWritable count :values){
                sum+=count.get();

            }
            v.set(sum);
            context.write(key,v);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration);

        job.setJarByClass(CharacterCount.class);

        job.setMapperClass(SplitMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));


        boolean result = job.waitForCompletion(true);


        System.exit(result?0:1);

    }

}
