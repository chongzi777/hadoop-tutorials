package com.se7en;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * <p>Description: 成绩统计</p>
 * <p>Company: 上海信而富企业管理有限公司</p>
 *
 * @author Qingdeng Chong
 * @verson v1.0
 * @date 05/31 031
 */
public class StudentScore implements Tool {

	public int run(String[] strings) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(StudentScore.class);
		job.setJobName("student_score_process");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job,new Path(strings[0]));
		FileOutputFormat.setOutputPath(job,new Path(strings[1]));
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public void setConf(Configuration configuration) {

	}

	public Configuration getConf() {
		Configuration conf = new Configuration();
		conf.addResource("configuration-default.xml");
		return conf;
	}

	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			System.out.println(line);
			StringTokenizer tokenizer = new StringTokenizer(line,"\n");
			while(tokenizer.hasMoreTokens()){
				StringTokenizer tokenizer1 = new StringTokenizer(tokenizer.nextToken());
				String stuName = tokenizer1.nextToken();
				String stuScore = tokenizer1.nextToken();
				Text name = new Text(stuName);
				int score = Integer.parseInt(stuScore);
				context.write(name,new IntWritable(score));
			}
		}
	}

	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;  //总分
			int count = 0;  //科目
			Iterator<IntWritable> iterator = values.iterator();
			while(iterator.hasNext()){
				sum += iterator.next().get();
				count++;
			}
			int avg = sum/count;
			context.write(key,new IntWritable(avg));
		}
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new StudentScore(),args);
		System.exit(ret);
	}
}
