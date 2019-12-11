package edu.nyu.tandon.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.shaded.org.apache.commons.io.output.ByteArrayOutputStream;

import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 *    K = filename inside the zip file
 *    V = bytes corresponding to the file
 *
 * ***/
public class NYUZRecordReader extends RecordReader<Text, BytesWritable> {

      private  FSDataInputStream fsDataInputStream;

      private ZipInputStream zipInputStream;

      private Text currentKey;

      private BytesWritable currentValue;

      private Boolean isFinished=false;


      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        // your code here
        // the code here depends on what/how you define a split....

         FileSplit fileSplit=(FileSplit) inputSplit;

         Configuration configuration=context.getConfiguration();

         Path path =fileSplit.getPath();
//          System.out.println("path in record reader is"+path.toString());

         FileSystem fileSystem=path.getFileSystem(configuration);

         fsDataInputStream=fileSystem.open(path);

         zipInputStream=new ZipInputStream(fsDataInputStream);

      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
//          System.out.println("in next key value");

         ZipEntry zipEntry=null;

         try{
            zipEntry=zipInputStream.getNextEntry();
         }
         catch (ZipException e){
            e.printStackTrace();
         }

         if(zipEntry==null){
            isFinished=true;
            return false;
         }
         //Key is the fileName
         currentKey=new Text(zipEntry.getName());

         ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream();

         byte[] buff=new byte[2048];

         while(true){
            int bytesRead=0;

            try{
               bytesRead=zipInputStream.read(buff,0,2048);
            }
            catch(EOFException e){

               return false;
            }

            if(bytesRead>0){
               byteArrayOutputStream.write(buff,0,bytesRead);
            }
            else
               break;
         }

         zipInputStream.closeEntry();

         currentValue=new BytesWritable(byteArrayOutputStream.toByteArray());


         return true; //prasa
      }

      @Override
      public Text getCurrentKey() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
//          System.out.println("key is "+currentKey.toString());
       return currentKey;//prasa
      }

      @Override
      public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....

       return currentValue; //prasa
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        // let's ignore this one for now
        return isFinished? 1:0;
      }

      @Override
      public void close() throws IOException {
        // your code here
        // the code here depends on what/how you define a split....

         try{
            zipInputStream.close();
            fsDataInputStream.close();
         }
         catch(Exception e){}
      }



}
