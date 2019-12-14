package com.lwx.domain;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Score implements WritableComparable<Score> {

    private String courseName;
    private String stuName;
    private Integer score;

    public Score() {
    }

    public Score(String courseName, String stuName, Integer score) {
        this.courseName = courseName;
        this.stuName = stuName;
        this.score = score;
    }

    public String getCourseName() {
        return courseName;
    }

    public void setCourseName(String courseName) {
        this.courseName = courseName;
    }

    public String getStuName() {
        return stuName;
    }

    public void setStuName(String stuName) {
        this.stuName = stuName;
    }

    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public int compareTo(Score o) {
        return score>o.getScore()?-1:1;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(courseName);
        dataOutput.writeUTF(stuName);
        dataOutput.writeInt(score);


    }

    public void readFields(DataInput dataInput) throws IOException {

        this.courseName=dataInput.readUTF();
        this.stuName=dataInput.readUTF();
        this.score=dataInput.readInt();
    }



    @Override
    public String toString() {
        return courseName+","+stuName+","+score;
    }
}
