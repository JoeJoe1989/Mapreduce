package pageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import pageRank.PageRank.PageRankMap;
import pageRank.PageRank.PageRankReduce;

public class IterativePageRank {
	
	public static String basePath;

	public static void iteratePageRank(int i) throws Exception {
		

		Configuration conf = new Configuration();

		Job job = new Job(conf, "PageRank" + i);

		job.setJarByClass(PageRank.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(PageRankMap.class);
		job.setReducerClass(PageRankReduce.class);

		FileInputFormat.addInputPath(job, new Path(basePath + i));
		int temp = i + 1;
		FileOutputFormat.setOutputPath(job, new Path(basePath + temp));

		boolean ret = job.waitForCompletion(true);
		if (!ret) {
			throw new Exception("Job Failed");
		}

	

	}

	public static void main(String[] args) throws Exception {
		int iterations = Integer.parseInt(args[0]);
		IterativePageRank.basePath = args[1];
		
		for (int i = 0; i < iterations; i++) {
			IterativePageRank.iteratePageRank(i);
		}

	}

}