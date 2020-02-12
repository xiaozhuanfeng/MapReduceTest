package com.company.wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
    /* Mapper */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /* Reducer */
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
            int sum = 0;
            for(IntWritable val : values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    /* 启动 MapReduce Job */
    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.home.dir","F:/resource/hadoop-2.7.0" );
        Configuration conf = new Configuration();
        /*if(args.length != 2){
            System.err.println("Usage: wordcount <int> <out>");
            System.exit(2);
        }*/

        //代码里写死,通常可以通过配置文件配置
        //String arg1 = "F:/xxxx/ideaProjects/MapReduceTest/input";
        //String arg2 = "F:/xxxx/ideaProjects/MapReduceTest/output";
        String arg1 = args[0];
        String arg2 = args[1];
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(arg1));
        FileOutputFormat.setOutputPath(job,new Path(arg2));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}