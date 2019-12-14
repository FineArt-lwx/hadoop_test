package com.lwx.mr;

import com.lwx.domain.Score;
import com.lwx.partition.CoursePartition;
import org.apache.commons.lang.ObjectUtils;
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


public class CourseScoreMR4 {

    public static class CsMapper extends Mapper<LongWritable, Text, Score, NullWritable> {

        Text k = new Text();


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = line.split(",");

            int tScore = 0;
            int num=0;

            for (int i = 2; i < data.length; i++) {
                int score = Integer.parseInt(data[i]);
                tScore += score;
                num++;
            }

            int avg = tScore / (data.length - 2);

            Score score = new Score(data[0], data[1], avg);


            context.write(score, NullWritable.get());


        }


    }


    public static class CsReducer extends Reducer<Score, NullWritable, Score, Text> {
        Text v = new Text();


        @Override
        protected void reduce(Score key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {



           for(NullWritable nl:values){
//               context.write(key,NullWritable.get());
               context.write(key,new Text());
           }




        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(CourseScoreMR4.class);

        job.setMapperClass(CsMapper.class);
        job.setReducerClass(CsReducer.class);

        job.setMapOutputKeyClass(Score.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Score.class);
        job.setOutputValueClass(NullWritable.class);

        job.setPartitionerClass(CoursePartition.class);
        job.setNumReduceTasks(4);


        FileInputFormat.setInputPaths(job, new Path("courseScoreInput3"));
        FileOutputFormat.setOutputPath(job, new Path("courseScoreOutput3"));


        boolean result = job.waitForCompletion(true);


        System.exit(result ? 0 : 1);


    }

}
