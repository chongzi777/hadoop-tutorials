package com.se7en;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * <p>Description: 排序</p>
 * <p>Company: 上海信而富企业管理有限公司</p>
 *
 * @author Qingdeng Chong
 * @verson v1.0
 * @date 06/20 020
 */
public class Sort {
	public static class SortMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
		private final static IntWritable data = new IntWritable();
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			data.set(Integer.parseInt(line));
			context.write(data,new IntWritable(1));
		}
	}

	public static class SortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		private static IntWritable linenum = new IntWritable(1);

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for(IntWritable val:values){
				context.write(linenum,key);
			}
			linenum =  new IntWritable(linenum.get() + 1);
		}
	}

	public static class SortPartition extends Partitioner<IntWritable,IntWritable>{

		public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
			int maxNumber = 65223;
			int bound = maxNumber/numPartitions + 1;
			int keyNumber = key.get();
			for(int i=0;i<numPartitions;i++) {
				if(keyNumber < bound*(i+1) && keyNumber>=bound*i){
					return i;
				}
			}
			return -1;
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.addResource("configuration-default.xml");

		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage:sort <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf,"sort");
		job.setJarByClass(Sort.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setPartitionerClass(SortPartition.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
