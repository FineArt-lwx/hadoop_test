package com.lwx.mr;

import com.lwx.domain.Score;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class CourseScoreMR2 {

    public static class CsMapper extends Mapper<LongWritable, Text, Text, Score> {

        Text k = new Text();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = line.split(",");

            Score score = new Score(data[0], data[1], Integer.parseInt(data[2]));
            k.set(data[0]+"\t"+data[2]);
            context.write(k, score);


        }


    }


    public static class CsReducer extends Reducer<Text, Score, Text, NullWritable> {
        Text k = new Text();
        String courseName;
        String score;



        @Override
        protected void reduce(Text key, Iterable<Score> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            int tScore = 0;
            StringBuffer sb=new StringBuffer();



            for (Score score : values) {


                count++;
                sb.append(score.getStuName()+",");
                courseName=score.getCourseName();
                tScore=score.getScore();

            }

            String names=sb.substring(0,sb.length()-1);


            k.set(key+"\t"+count+"\t"+names);



            if(count>1){
                context.write(k, NullWritable.get());

            }


        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(CourseScoreMR2.class);

        job.setMapperClass(CsMapper.class);
        job.setReducerClass(CsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Score.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileInputFormat.setInputPaths(job, new Path("courseScoreInput1"));
        FileOutputFormat.setOutputPath(job, new Path("courseScoreOutput1"));


        boolean result = job.waitForCompletion(true);


        System.exit(result ? 0 : 1);


    }

}
