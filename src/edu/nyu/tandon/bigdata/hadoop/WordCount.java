package edu.nyu.tandon.bigdata.hadoop;

/**
 * Created by prasannasurianarayanan on 02/10/19.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

public class WordCount {

    public static void main(String[] args) throws Exception {

//        NYUZInputFormat nyuzInputFormat =new NYUZInputFormat();
//        nyuzInputFormat.setup();

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        @SuppressWarnings("deprecation")


        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);

        //based on command line argument , choose which program to run
        if(otherArgs[2].equals("zip")){
            System.out.println("in zip");
            job.setMapperClass(zipMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setInputFormatClass(ZipInputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            ZipInputFormat.addInputPath(job,new Path(otherArgs[0]));
        }

        else if(otherArgs[2].equals("normal")){
            // for part 1
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        }
        else if (otherArgs[2].equals("json")){
            job.setMapperClass(JSONMapper.class);
            job.setCombinerClass(JSONReducer.class);
            job.setReducerClass(JSONReducer.class);
            job.setInputFormatClass(JSONInputFormat.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            JSONInputFormat.addInputPath(job,new Path(otherArgs[0]));
        }


        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        int row=0;
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            row++;

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {

                word.set(itr.nextToken());

                if(word.toString().equalsIgnoreCase("door")){
//                    System.out.println(value.toString());
                    Text line=new Text(value.toString());
                    IntWritable line_number=new IntWritable(row);
                    context.write(line,line_number);

                }

            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

//            System.out.println(key.toString());
            for (IntWritable val : values) {
//                System.out.println(val.get());
                sum = val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    //references for this program  :http://cutler.io/2012/07/hadoop-processing-zip-files-in-mapreduce/
    public static class zipMapper extends Mapper<Text,BytesWritable,Text,IntWritable>{

        private final static IntWritable one = new IntWritable( 1 );
        private Text word = new Text();

        public void map( Text key, BytesWritable value, Context context )
                throws IOException, InterruptedException
        {

            String filename = key.toString();

            int row=0;

            // Prepare the content

            String content = new String( value.getBytes(), "UTF-8" );
            //content = content.replaceAll( "[^A-Za-z \n]", "" ).toLowerCase();

            // go line by line in the file
            Scanner scanner=new Scanner(content);
            while (scanner.hasNextLine()){
//                System.out.println("in next line "+key.toString());
                row++;
                String currentline=scanner.nextLine();
                StringTokenizer itr=new StringTokenizer(currentline);

                while(itr.hasMoreTokens()){
                    String currentword=itr.nextToken();
                    //check for door in the line
                    if(currentword.equalsIgnoreCase("door")){
                        Text full_line=new Text(currentline);
                        IntWritable line_number=new IntWritable(row);
                        context.write(full_line,line_number);
                    }
                }
            }
        }
    }

    public static class JSONMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
        private Text word=new Text();
        private LongWritable number=new LongWritable();
        public void map( LongWritable key, Text value, Context context )
                throws IOException, InterruptedException
        {
            // NOTE: the filename is the *full* path within the ZIP file
            // e.g. "subdir1/subsubdir2/Ulysses-18.txt"

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {

                word.set(itr.nextToken());

//                if(word.toString().equalsIgnoreCase("door")){
                    System.out.println("key is "+key.toString());
                    System.out.println("value is "+value.toString());

                Text line=new Text("value is "+value.toString());
//                    Lon line_number=new IntWritable(row);
                    context.write(key,word);

//                }

            }
        }
    }
    public static class JSONReducer extends Reducer<LongWritable,Text,LongWritable,Text> {

        private LongWritable result = new LongWritable();

        public void reduce(LongWritable key, Text values,
                           Context context
        ) throws IOException, InterruptedException {
            long sum = 0;

//            System.out.println(key.toString());
//            System.out.println(values.toString());
//            for (LongWritable val : values) {
//                System.out.println(val.get());
//                sum = val.get();
//            }
            result.set(sum);
            context.write(key, values);
        }
    }


}
