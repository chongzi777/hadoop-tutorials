package com.se7en;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * <p>Description: 多表连接</p>
 * <p>Company: 上海信而富企业管理有限公司</p>
 *
 * @author Qingdeng Chong
 * @verson v1.0
 * @date 06/22 022
 */
public class MTJoin {

	public static int time = 0;

	public static class Map extends Mapper<Object,Text,Text,Text>{
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String relation = new String();

			if(line.contains("factoryName") || line.contains("addressName")){
				return;
			}
			StringTokenizer st = new StringTokenizer(line);
			String mapKey = new String();
			String mapValue = new String();
			int i = 0;
			while(st.hasMoreTokens()){
				String token = st.nextToken();
				if(token.charAt(0)>='0' && token.charAt(0)<='9'){
					mapKey = token;
					if(i>0){
						relation = "1";
					}else{
						relation = "2";
					}
					continue;
				}
				mapValue += token + " ";
				i++;
			}
			context.write(new Text(mapKey),new Text(relation+"+"+mapValue));
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(time == 0){
				context.write(new Text("factoryName"),new Text("addressName"));
				time ++;
			}
			int factoryNum = 0;
			String[] factoryNames = new String[10];
			int addressNum = 0;
			String[] addressNames = new String[10];
			Iterator it = values.iterator();
			while(it.hasNext()){
				String record = it.next().toString();
				if(record.length()==0){
					continue;
				}
				char relation = record.charAt(0);
				if('1' == relation){
					factoryNames[factoryNum] = record.substring(2);
					factoryNum++;
				}else{
					addressNames[addressNum] = record.substring(2);
					addressNum++;
				}
			}
			if(addressNum > 0 && factoryNum > 0){
				for (int i =0;i<factoryNum;i++){
					for (int j=0;j<addressNum;j++){
						context.write(new Text(factoryNames[i]),new Text(addressNames[j]));
					}
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.addResource("configuration-default.xml");

		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage:multiple table join <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf,"multiple table join");
		job.setJarByClass(MTJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
