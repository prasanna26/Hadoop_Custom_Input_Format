package edu.nyu.tandon.bigdata.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by prasannasurianarayanan on 08/10/19.
 */
//public class JSONInputFormat extends FileInputFormat<Text,BytesWritable> {
//
//    protected boolean isSplitable(JobContext context, Path filename) {
//        return false;
//    }
//
//    @Override
//    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
////        System.out.println("in inputformat");
//        return new NYUZRecordReader();
//    }
//}


public class JSONInputFormat extends FileInputFormat<LongWritable,Text> {

    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
//        System.out.println("in inputformat");
        return new JSONRecordReader();
    }
}