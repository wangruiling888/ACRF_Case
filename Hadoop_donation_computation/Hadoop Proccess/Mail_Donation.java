import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Mail_Donation {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable >{

		@Override
		protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
				
				String [] tokens = value.toString().split("\t");
				String id = tokens[0];
				int id_1 = (int)Double.parseDouble(id);
				double amount = Double.parseDouble(tokens[1]);
				System.out.println(id_1 + "   " + amount);
				context.write(new Text(String.valueOf(id_1)), new DoubleWritable(amount));
		}
		
	}
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {
			double amountSum = 0;
			int count = 0;
			System.out.println(key);
			for (DoubleWritable value:values) {
				System.out.println(value.get() + "---------------");
				amountSum += value.get();
				count += 1;
			}
			System.out.println(String.valueOf(amountSum) + "\t" + String.valueOf(count));
			context.write(key, new Text(String.valueOf(amountSum) + "\t" + String.valueOf(count)));
			
		}
		
	}
	public static void main(String[] args)throws Exception {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
//				System.out.println("lalalalal");
				Configuration conf = new Configuration();
				Job job = Job.getInstance(conf, "Donation");
				job.setMapperClass(MyMapper.class);
				job.setReducerClass(MyReducer.class);
				//job.setCombinerClass(MyReducer.class);
				System.out.println("lalalalal");
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(DoubleWritable.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				job.waitForCompletion(true);
	}

}
