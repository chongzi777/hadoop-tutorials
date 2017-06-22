package com.se7en;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * <p>Description: </p>
 * <p>Company: 上海信而富企业管理有限公司</p>
 *
 * @author Qingdeng Chong
 * @verson v1.0
 * @date 05/25 025
 */
public class WordCount_oldApi {

    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                outputCollector.collect(word,one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{

        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while(iterator.hasNext()){
                sum += iterator.next().get();
            }
            outputCollector.collect(text,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "D:\\IDE\\hadoop-2.7.3");
        JobConf conf = new JobConf(WordCount_oldApi.class);

        conf.setJobName("wordCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));

        JobClient.runJob(conf);
    }
}
