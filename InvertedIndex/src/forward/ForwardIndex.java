package forward;

import inverted.CombinedOccurence;
import inverted.InvertedIndex;
import inverted.Occurence;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ForwardIndex {
	static int numberOfNodes = 3;
	static BigInteger total = new BigInteger(
			"ffffffffffffffffffffffffffffffffffffffff", 16);
	static BigInteger unit = total.divide(BigInteger.valueOf(numberOfNodes));;

	public static class Map extends
			Mapper<NullWritable, BytesWritable, Text, Text> {
		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
		}
	}

	private static CombinedOccurence combine(
			ArrayList<Occurence> occurences) {
		CombinedOccurence res = new CombinedOccurence(occurences.get(0).url);
		for (Occurence occurence : occurences) {
			res.tf += occurence.importance;
			res.addPosition(occurence.position);	
		}
		return res;
	}

	private static boolean isAllCapital(String s) {
		for (int i = 0; i < s.length(); i++) {
			if (!Character.isUpperCase(s.charAt(i)))
				return false;
		}
		return true;
	}

	private static boolean isAllLetter(String s) {
		for (int i = 0; i < s.length(); i++) {
			if (!Character.isLetter(s.charAt(i)))
				return false;
		}
		return true;
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
			super.setup(context);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			super.cleanup(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String url = key.toString();
			HashMap<String, CombinedOccurence> hm = new HashMap<String, CombinedOccurence>();
			for (Text value: values) {
				String[] entry = value.toString().split(",");
				String word = entry[0];
				double importance = Double.parseDouble(entry[1]);
				int position = Integer.parseInt(entry[2]);
				if (hm.containsKey(word)) {
					CombinedOccurence temp = hm.get(word);
					temp.tf += importance;
					temp.addPosition(position);
				} else {
					CombinedOccurence temp = new CombinedOccurence(url);
					temp.tf += importance;
					temp.addPosition(position);
					hm.put(word, temp);
				}	 
			}
			
			double max = 0;
			for (String word: hm.keySet()) {
				max = Math.max(max, hm.get(word).tf);
			}
			for (String word: hm.keySet()) {
				CombinedOccurence temp = hm.get(word);
				temp.tf = 0.5 + 0.5 * temp.tf / max;
				keyInfo.set("Word\t" + word);
				valueInfo.set(temp.toString());

				context.write(keyInfo, valueInfo);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "InvertedIndex");

		job.setJarByClass(InvertedIndex.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		for (int i = 0; i < numberOfNodes; i++) {
			MultipleOutputs.addNamedOutput(job, "output" + i,
					TextOutputFormat.class, Text.class, Text.class);
		}
		MultipleOutputs.addNamedOutput(job, "links", TextOutputFormat.class,
				Text.class, Text.class);

		boolean ret = job.waitForCompletion(true);
		if (!ret) {
			throw new Exception("Job Failed");
		}

	}
}
