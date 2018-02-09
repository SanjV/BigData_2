package friends;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mfs {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text word1 = new Text(); // type of output key
            Text word2 = new Text(); // type of output key
            String[] splits = value.toString().split("\t");
            String s = "";
            try{
                s = splits[1];
                String[] flist = s.split(",");
                word2.set(splits[0]);
                for(String f : flist){
                    word1.set(f);
                    context.write(word1, word2);
                }

            }catch (Exception e){
                s= "";
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text result = new Text();
            String s = "";
            for (Text data : values){

                s = s+data.toString()+",";

            }
            result.set(s);
            context.write(key,result); // create a pair <keyword, number of occurences>
        }
    }

    public static class Map2
            extends Mapper<LongWritable, Text, Text, Text>{


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text word1 = new Text(); // type of output key
            Text word2 = new Text(); // type of output key
            String[] splits = value.toString().split("\t");
            String s = "";
            try{
                s = splits[1];
                String[] flist1 = s.split(",");
                String[] flist2 = s.split(",");
                ArrayList<String> arr = new ArrayList<>();
                for(int j = 0;j < flist1.length; j++){
                    for(int i = 0;i < flist2.length; i++){
                        int mf1 = Integer.parseInt(flist1[j]);
                        int mf2 = Integer.parseInt(flist2[i]);
                        String fpair = "";
                        if(mf1 > mf2){
                            fpair = flist2[i]+","+flist1[j];
                        }else if(mf1 < mf2){
                            fpair = flist1[j]+","+flist2[i];
                        }
                        if(!fpair.equals("") && !arr.contains(fpair)){
                            word1.set(fpair);
                            word2.set(splits[0]);
                            arr.add(fpair);
                            context.write(word1,word2);
                        }
                    }
                }

            }catch (Exception e){
                s= "";
            }
        }
    }

    public static class Reduce2
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text result = new Text();
            String s = "";
            ArrayList<String> keys = new ArrayList<>();
            keys.add("0,4");
            keys.add("20,22939");
            keys.add("1,29826");
            keys.add("6222,19272");
            keys.add("28041,28056");

            if(keys.contains(key.toString())){

                for (Text data : values){

                    s = s+data.toString()+",";

                }
                result.set(s);
                context.write(key,result);


            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


        // create a job with name "wordcount"
        Job job = new Job(conf, "Mfriends1");
        job.setJarByClass(mfs.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(job.waitForCompletion(true)){
            Job job2 = new Job(conf, "Mfriends2");
            job2.setJarByClass(mfs.class);
            job2.setMapperClass(Map2.class);
            job2.setReducerClass(Reduce2.class);

            // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

            // set output key type
            job2.setOutputKeyClass(Text.class);
            // set output value type
            job2.setOutputValueClass(Text.class);
//            job2.setSortComparatorClass(DescendingKeyComparator.class);
            //set the HDFS path of the input data
            FileInputFormat.addInputPath(job2, new Path(args[1]+"/part-r-00000"));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/o2"));

            System.exit(job2.waitForCompletion(true)? 1 : 0);
        }

        System.exit(1);

    }


}
