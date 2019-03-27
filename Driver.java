package Maincode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	StringBuilder str = new StringBuilder();

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Map Reduce Project2");
		job.setJarByClass(Driver.class);
		job.setMapperClass(MapperTask.class);
		job.setReducerClass(ReducerTask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class MapperTask extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String token = value.toString();
		String[] tokens = token.split(",");

		if (tokens[0] != "#DATE") {

			//  Date on which maximum number of accidents took place.
			String key1 = "Date_" + tokens[0];
			int killed = Integer.parseInt(tokens[3]) + Integer.parseInt(tokens[4]) + Integer.parseInt(tokens[5])
					+ Integer.parseInt(tokens[6]) + Integer.parseInt(tokens[7]) + Integer.parseInt(tokens[8])
					+ Integer.parseInt(tokens[9]) + Integer.parseInt(tokens[10]);

			context.write(new Text(key1.toString()), new IntWritable(killed));

			// Borough with maximum count of accident fatality
			String key2 = "Boro_" + tokens[1];
			int personsFatality = Integer.parseInt(tokens[4]) + Integer.parseInt(tokens[6])
					+ Integer.parseInt(tokens[8]) + Integer.parseInt(tokens[10]);

			context.write(new Text(key2), new IntWritable(personsFatality));

			// Zip with maximum count of accident fatality
			String key3 = "ZipC_" + tokens[2];
			int zipCodeFatality = Integer.parseInt(tokens[4]) + Integer.parseInt(tokens[6])
					+ Integer.parseInt(tokens[8]) + Integer.parseInt(tokens[10]);
			context.write(new Text(key3), new IntWritable(zipCodeFatality));

			// Which vehicle type is involved in maximum accidents
			String key4 = "Vehi_" + tokens[11];
			int Vehicle_with_MaximunAccidents = Integer.parseInt(tokens[3]) + Integer.parseInt(tokens[4])
					+ Integer.parseInt(tokens[5]) + Integer.parseInt(tokens[6]) + Integer.parseInt(tokens[7])
					+ Integer.parseInt(tokens[8]) + Integer.parseInt(tokens[9]) + Integer.parseInt(tokens[10]);
			context.write(new Text(key4), new IntWritable(Vehicle_with_MaximunAccidents));

			String[] year = tokens[0].split("/");

			// Year in which maximum Number Of Persons and Pedestrians Injured
			String key5 = "ppdi_" + year[2];
			int persons_pedestrains_injured = Integer.parseInt(tokens[3]) + Integer.parseInt(tokens[5]);
			context.write(new Text(key5), new IntWritable(persons_pedestrains_injured));

			// Year in which maximum Number Of Persons and Pedestrians Killed
			String key6 = "ppdk_" + year[2];
			int persons_pedestrains_killed = Integer.parseInt(tokens[4]) + Integer.parseInt(tokens[6]);
			context.write(new Text(key6), new IntWritable(persons_pedestrains_killed));

			// Year in which maximum Number Of Cyclist Injured and Killed (combined)
			String key7 = "cyik_" + year[2];
			int cyclist = Integer.parseInt(tokens[7]) + Integer.parseInt(tokens[8]);
			context.write(new Text(key7), new IntWritable(cyclist));

			// Year in which maximum Number Of Motorist Injured and Killed (combined)
			String key8 = "moik_" + year[2];
			int motorists = Integer.parseInt(tokens[9]) + Integer.parseInt(tokens[10]);
			context.write(new Text(key8), new IntWritable(motorists));

		}

	}

}

class ReducerTask extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		IntWritable intWritable1 = new IntWritable();
		Text text1 = new Text();
		int sumVal = 0;

		for (IntWritable val : values) {
			sumVal += val.get();
		}
		text1.set(key);
		intWritable1.set(sumVal);
		context.write(text1, intWritable1);

	}

}
