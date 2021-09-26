import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*14 var*/

public class ThirdTask {

	public static class FiveMinuteMapper extends Mapper<Object, Text, Text, Text> {

		
		private Text keyT = new Text();
		private Text valueT = new Text();

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			try{
				FileSplit fileSplit = (FileSplit)context.getInputSplit();
				String filename = fileSplit.getPath().getName();
				
				if(filename.split("\\.")[0].equals("userlog")) {
					String h = filename.split("\\.")[1];
					String u = filename.split("\\.")[2];
					int minute = (((int)Double.parseDouble(tokens[1]))/60/5)*5;
					keyT.set("usr|"+u+"|"+h+"|"+minute+"|");
					valueT.set(tokens[2]+"|"+"1");
					if(tokens[0].equals("7")) {
						context.write(keyT, valueT);
					}
				}else if(filename.split("\\.")[0].equals("stationlog")) {
					String h = filename.split("\\.")[1];
					int minute = (((int)Double.parseDouble(tokens[1])-60*60*24*6)/60/5)*5;
					int day = (((int)Double.parseDouble(tokens[1]))/60/60/24)+1;
					keyT.set("sta|"+h+"|"+minute+"|");
					valueT.set(tokens[2]+"|"+"1");
					if(day == 7) {
						context.write(keyT, valueT);
					}
				}
				
			}catch(Exception e){
			/*	e.printStackTrace();
				keyT.set("0|0|0|");
				valueT.set("0.0"+"|"+"1");
				context.write(keyT, valueT);*/
			}
		}
	}
	

	public static class SummFiveMinuteReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Double sum = 0d;
			int amount = 0;
			for (Text val : values) {
				System.out.println(val.toString());
				String[] valArr = val.toString().split("\\|");
				System.out.println(valArr[0]);
				System.out.println(valArr[1]);
				Double curval = Double.parseDouble(valArr[0]);
				int cur_amount = Integer.parseInt(valArr[1]);
				sum += curval;
				amount += cur_amount;
			}
			result.set(sum+"|"+amount);
			context.write(key, result);
		}
	}

	
	public static class JoiningMapper extends Mapper<Object, Text, Text, Text> {

		
		private Text keyT = new Text();
		private Text valueT = new Text();

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\|");
			try{
				String type = tokens[0];
				if(type.equals("usr")) {
					String h = tokens[2];
					String minute = tokens[3];
					keyT.set(h+"|"+minute+"|");
					Double avg = Double.parseDouble(tokens[4])/Integer.parseInt(tokens[5]);
					valueT.set(avg+"|0.0");
				}else if(type.equals("sta")) {
					String h = tokens[1];
					String minute = tokens[2];
					keyT.set(h+"|"+minute+"|");
					Double avg = Double.parseDouble(tokens[3])/Integer.parseInt(tokens[4]);
					valueT.set("0.0|"+avg);
				}
				context.write(keyT, valueT);
			}catch(Exception e){
				/*e.printStackTrace();
				keyT.set("0|0|");
				valueT.set("0.0"+"|"+"1");
				context.write(keyT, valueT);*/
			}
		}
	}
	
	
	public static class JoiningReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Double speed_sum = 0d;
			Double errors_sum = 0d;
			for (Text val : values) {
				String[] valArr = val.toString().split("\\|");
				
				Double speed = Double.parseDouble(valArr[0]);
				Double errors = Double.parseDouble(valArr[1]);
				speed_sum += speed;
				errors_sum += errors;
			}
			result.set(speed_sum+"|"+errors_sum);
			context.write(key, result);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "avg five minute speed");
		job.setJarByClass(ThirdTask.class);
		job.setMapperClass(FiveMinuteMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(SummFiveMinuteReducer.class);
		job.setReducerClass(SummFiveMinuteReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/first"));
		
		job.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "avg day speed");
		job2.setJarByClass(ThirdTask.class);
		job2.setMapperClass(JoiningMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setCombinerClass(JoiningReducer.class);
		job2.setReducerClass(JoiningReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]+"/first"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/second"));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}

}
