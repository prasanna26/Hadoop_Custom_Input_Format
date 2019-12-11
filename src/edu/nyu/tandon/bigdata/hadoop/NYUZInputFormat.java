package edu.nyu.tandon.bigdata.hadoop;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.shaded.org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.util.StringUtils;

import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Extends the basic FileInputFormat to accept ZIP files.
 * ZIP files are not 'splittable', so we need to process/decompress in place:
 * each ZIP file will be processed by a single Mapper; we are parallelizing files, not lines...
 */
public class NYUZInputFormat extends InputFormat<Text, BytesWritable> {

    private static Path zip_path;
    private static Configuration configuration;
    private static FileSystem filesystem;
    private FSDataInputStream fsin;
    private ZipInputStream zip;
    @Override
    public List<InputSplit> getSplits(JobContext context)
            throws IOException, InterruptedException {

        // your code here
        // our splits will consist of whole files found insize our input zip file
        // return splits....
        System.out.println("trying splits");

        ArrayList<InputSplit> splits=new ArrayList<>();
        System.out.println("Path in get splits is "+zip_path.toString());
        fsin=filesystem.open(zip_path);

        zip=new ZipInputStream(fsin);
        System.out.println("zip created");
        ZipEntry entry;
//        byte[] buff=new byte[2048];
        long current_pos=0;
        long begin_pos=0;
        while((entry=zip.getNextEntry())!=null) {
            begin_pos=current_pos;
            System.out.println("file is" + entry.getName());

            int length = 0;
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

            byte[] buff = new byte[2048];

            while (true) {
                int bytesRead = 0;

                try {
                    bytesRead = zip.read(buff, 0, 2048);
                } catch (EOFException e) {

                }

                if (bytesRead > 0) {
                    byteArrayOutputStream.write(buff, 0, bytesRead);
                }
                else
                    break;

                current_pos+=bytesRead;
            }
            System.out.println("file written");


//            splits.add(new FileSplit(zip_path,begin_pos,current_pos));


        }
        return null;
    }

    /*** return a record reader
     *
     * @param split
     * @param context
     * @return (Text,BytesWritable)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        // no need to modify this one....
        System.out.println("going to call recordreader");

        return new NYUZRecordReader();
    }

    public static void addInputPath(Job job, Path path) throws IOException {
        Configuration conf = job.getConfiguration();
        configuration=conf;
        filesystem=path.getFileSystem(conf);
        path = path.getFileSystem(conf).makeQualified(path);
        zip_path=path;
        String dirStr = StringUtils.escapeString(path.toString());
        System.out.println(dirStr);
        char[][] grid=new char[4][5];
        int[][] tepm=new int[4][];
        int[] rows={1,2,3};
        System.out.println(grid[4].length);
        Set<String> hashsset=new HashSet<>();

        String dirs = conf.get("mapreduce.input.fileinputformat.inputdir");
        conf.set("mapreduce.input.fileinputformat.inputdir", dirs == null?dirStr:dirs + "," + dirStr);
    }

    public class TestSplit extends InputSplit{

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }

        public int check(){

            HashSet<String> visited=new HashSet<>();
            Queue<String> queue=new LinkedList<>();
            String start="0000";
            String target="1234";
            queue.offer(start);
            int turns=0;
            while(!queue.isEmpty()){

                for(int i=0;i<queue.size();i++){
                    String current=queue.poll();

                    if(!visited.add(current)) continue;
                    if(current.equals(target)){
                        return turns;
                    }
                    for(String locks:nextlocks(current)){
                        if(!visited.contains(locks))
                            queue.offer(locks);

                    }


                }
                turns++;
            }
            return 1;
        }

    }

    public ArrayList<String> nextlocks(String current){
        char[] temp=current.toCharArray();
        ArrayList<String> locks=new ArrayList<>();

        int k=(int)(Math.sqrt(17));
        Math.pow(k,2);
        PriorityQueue<Integer> pq=new PriorityQueue<Integer>();
        pq.add(1);
        pq.add(5);
        pq.add(-1);
        pq.add(3);
        return locks;
    }




}
