package com.lwx.partition;

import com.lwx.domain.Score;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CoursePartition extends Partitioner<Score, NullWritable> {



    @Override
    public int getPartition(Score score, NullWritable nullWritable, int i) {
        String courseName = score.getCourseName();
        if(courseName.equals("computer")){
            return 0;
        }else if(courseName.equals("english")){
            return 1;
        }else if(courseName.equals("algorithm")){
            return 2;
        }else {
            return 3;
        }



    }
}
