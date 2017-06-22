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

/**
 * <p>Description:单表关联</p>
 * <p>Company: 上海信而富企业管理有限公司</p>
 *
 * @author Qingdeng Chong
 * @verson v1.0
 * @date 06/21 021
 */
public class STJoin {
	public static int time = 0;

	public static class Map extends Mapper<Object,Text,Text,Text>{
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String relationType = new String();
			String line = value.toString();
			String[] values = {line.split(" ")[0],line.split(" ")[1]};
			if(values[0].compareTo("child")!=0){
				relationType = "1";  //左右表区分标志
				context.write(new Text(values[1]),new Text(relationType + "-" + new Text(values[0])));
				relationType = "2";
				context.write(new Text(values[0]),new Text(relationType + "-" + new Text(values[1])));
			}
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(time == 0){
				context.write(new Text("grandchild"),new Text("grandparent"));
				time ++;
			}
			int grandChildNum = 0;
			String grandChild[] = new String[10];
			int grandParentNum = 0;
			String grandParent[] = new String[10];
			Iterator it = values.iterator();
			while (it.hasNext()){
				String record = it.next().toString();
				String relationType = record.split("-")[0];
				String relationName = record.split("-")[1];
				if("1".equals(relationType)){
					grandChild[grandChildNum] = relationName;
					grandChildNum++;
				}else{
					grandParent[grandParentNum] = relationName;
					grandParentNum++;
				}
			}
			if(grandChildNum != 0 && grandParentNum != 0){
				for(int m = 0;m<grandChildNum;m++){
					for (int n=0;n<grandParentNum;n++){
						context.write(new Text(grandChild[m]),new Text(grandParent[n]));
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
			System.err.println("Usage:single table join <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf,"single table join");
		job.setJarByClass(STJoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
