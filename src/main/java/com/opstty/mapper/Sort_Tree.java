package com.opstty.mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Sort_Tree extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    private DoubleWritable height = new DoubleWritable();
    private Text treeData = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into columns
        String[] columns = value.toString().split("\t");
        if (columns.length > 6) {
            try {
                // Extract the "HAUTEUR" value (seventh column)
                double treeHeight = Double.parseDouble(columns[6]);
                height.set(treeHeight);
                treeData.set(value);
                context.write(height, treeData);
            } catch (NumberFormatException e) {
                // Ignore lines with invalid height values
            }
        }
    }
}
