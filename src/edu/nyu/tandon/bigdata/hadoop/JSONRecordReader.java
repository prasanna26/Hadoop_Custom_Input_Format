package edu.nyu.tandon.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.shaded.org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.util.LineReader;

import java.io.EOFException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

/**
 * Created by prasannasurianarayanan on 08/10/19.
 */
public class JSONRecordReader extends RecordReader<LongWritable,Text> {

    private long start;
    private long end;
    private long pos;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key;
    private Text value=new Text();

    private FSDataInputStream fsDataInputStream;

    private ZipInputStream zipInputStream;

    private Text currentKey;

    private BytesWritable currentValue;

    private Boolean isFinished=false;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

        // your code here
        // the code here depends on what/how you define a split....
        System.out.println("in initializer");
        FileSplit fileSplit=(FileSplit) inputSplit;

        Configuration configuration=context.getConfiguration();
        this.maxLineLength=configuration.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);


        start=fileSplit.getStart();
        end=start+fileSplit.getLength();

        final Path file=fileSplit.getPath();
        System.out.println("path is "+file.toString());
        FileSystem fileSystem=file.getFileSystem(configuration);
        FSDataInputStream filein=fileSystem.open(fileSplit.getPath());
        fsDataInputStream=fileSystem.open(file);
        boolean skipFirstLine=false;
        if(start!=0){
            skipFirstLine=true;
            --start;
            filein.seek(start);
        }

        in=new LineReader(filein,configuration);

        if(skipFirstLine){
            Text dummy=new Text();
            start+=in.readLine(dummy,0,(int)Math.min((long)Integer.MAX_VALUE,end-start));
        }

        this.pos=start;
        System.out.println("pos is "+pos);

//        Path path =fileSplit.getPath();
//        System.out.println("path in record reader is"+path.toString());
//
//        FileSystem fileSystem=path.getFileSystem(configuration);
//
//        fsDataInputStream=fileSystem.open(path);


//        zipInputStream=new ZipInputStream(fsDataInputStream);

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        System.out.println("in nextkey value");

        System.out.println("key:  "+key.toString());
        key.set(pos);
        int newsize=0;

//        ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream();
//        byte[]

        while(pos<end){
            newsize=in.readLine(value,maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE,end-pos),maxLineLength));
            System.out.println(value.toString());
            if(value.toString().contains("{")){
                System.out.println(value);
            }

            if(newsize==0){
                break;

            }

            pos+=newsize;
            if(newsize<maxLineLength){
                break;
            }
        }

        if(newsize==0){
            System.out.println("size 0");
            key=null;
            value=null;
            return false;
        }
        else{
            return true;
        }
//        System.out.println("in next key value");
//
//        ZipEntry zipEntry=null;
//
//        try{
//            zipEntry=zipInputStream.getNextEntry();
//        }
//        catch (ZipException e){
//            e.printStackTrace();
//        }
//
//        if(zipEntry==null){
//            isFinished=true;
//            return false;
//        }
//
//        //Key is the fileName
//        currentKey=new Text(zipEntry.getName());
//
//        ByteArrayOutputStream byteArrayOutputStream=new ByteArrayOutputStream();
//
//        byte[] buff=new byte[2048];
//
//        while(true){
//            int bytesRead=0;
//
//            try{
//                bytesRead=zipInputStream.read(buff,0,2048);
//            }
//            catch(EOFException e){
//
//                return false;
//            }
//
//            if(bytesRead>0){
//                byteArrayOutputStream.write(buff,0,bytesRead);
//            }
//            else
//                break;
//        }
//
//        zipInputStream.closeEntry();
//
//        currentValue=new BytesWritable(byteArrayOutputStream.toByteArray());
//
//
//        return true; //prasa
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        return key;//prasa
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        // your code here
        // the code here depends on what/how you define a split....
        System.out.println("value in getval is "+value.toString());
        return value; //prasa
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        // let's ignore this one for now
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        // your code here
        // the code here depends on what/how you define a split....

        if(in!=null){
            in.close();
        }
    }

}
