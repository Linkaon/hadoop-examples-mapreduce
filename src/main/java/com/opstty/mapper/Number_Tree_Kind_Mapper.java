package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Number_Tree_Kind_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text treeKind = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into columns
        String[] columns = value.toString().split("\t");
        if (columns.length > 2) {
            // Extract the "GENRE" value (third column)
            treeKind.set(columns[2]);
            context.write(treeKind, one);
        }
    }
}