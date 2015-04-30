package pageRank;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class PageRank {

	public static class PageRankMap extends
			Mapper<LongWritable, Text, Text, Text> {
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String url = line.split("\\s+", 3)[0];
			String urlInfo = line.split("\\s+", 3)[1];
			String numOfOutLinks = urlInfo.split(",")[0];
			String myValue = line.split("\\s+", 3)[2];

			if (!"0".equals(numOfOutLinks)) {

				keyInfo.set(url);
				valueInfo.set("||" + numOfOutLinks + "\t" + myValue);
				context.write(keyInfo, valueInfo);

				String[] outLinks = myValue.split("\\|\\|");
				for (String outLinkEntry : outLinks) {
					String outLink = outLinkEntry.split(",")[0];
					String times = outLinkEntry.split(",")[1];
					keyInfo.set(outLink);
					valueInfo.set(times + "," + urlInfo);
					context.write(keyInfo, valueInfo);
				}

			}
		}
	}

	public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		private final double decay = 0.85;

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// for (Text value: values) {
			// context.write(key, value);
			// }
			float pageRankSum = 0;
			String outLinks = "0,1||";
			String myNumOfOutLinks = "0";
			for (Text value : values) {
				String myValue = value.toString();
				if (myValue.startsWith("||")) {
					myNumOfOutLinks = myValue.substring(2).split("\t", 2)[0];
					outLinks = myValue.substring(2).split("\t", 2)[1];
				} else {
					String[] info = myValue.split(",");
					double appearTimes = Double.parseDouble(info[0]);
					double numOfOutLinks = Double.parseDouble(info[1]);
					double pageRank = Double.parseDouble(info[2]);
					pageRankSum += 1 - decay + decay * appearTimes
							/ numOfOutLinks * pageRank;
				}
			}
			keyInfo.set(key + "\t" + myNumOfOutLinks + "," + pageRankSum);
			valueInfo.set(outLinks);
			context.write(keyInfo, valueInfo);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PageRank");

		job.setJarByClass(PageRank.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(PageRankMap.class);
		job.setReducerClass(PageRankReduce.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean ret = job.waitForCompletion(true);
		if (!ret) {
			throw new Exception("Job Failed");
		}

	}
}
