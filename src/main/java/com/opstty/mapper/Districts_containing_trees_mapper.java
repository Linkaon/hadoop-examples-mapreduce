package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Districts_containing_trees_mapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text arrondissement = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into columns
        String[] columns = value.toString().split("\t");
        if (columns.length > 1) {
            // Extract the "ARRONDISSEMENT" value (second column)
            arrondissement.set(columns[1]);
            context.write(arrondissement, NullWritable.get());
        }
    }
}
