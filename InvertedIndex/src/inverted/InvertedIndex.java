package inverted;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
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

public class InvertedIndex {
	static int numberOfNodes = 3;
	static BigInteger total = new BigInteger(
			"ffffffffffffffffffffffffffffffffffffffff", 16);
	static BigInteger unit = total.divide(BigInteger.valueOf(numberOfNodes));;

	public static class Map extends
			Mapper<NullWritable, BytesWritable, Text, Text> {

		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			HashMap<String, ArrayList<Occurence>> wordOccurence = new HashMap<String, ArrayList<Occurence>>();

			Text keyInfo = new Text();
			Text valueInfo = new Text();

			Path filePath = ((FileSplit) context.getInputSplit()).getPath();
			String url = filePath.getName();

			byte[] fileContentByte = value.getBytes();

			String html = new String(fileContentByte);
			Document doc = Jsoup.parse(html);

			int position = 0;

			// for meta data
			Elements metaData = doc.select("meta[name=description|keywords]");
			if (metaData != null) {
				String metaContent = "";
				for (Element data : metaData) {
					metaContent += data.attr("content") + " ";
				}

				StringTokenizer metaTokens = new StringTokenizer(metaContent);
				while (metaTokens.hasMoreTokens()) {
					String word = metaTokens.nextToken();
					helper(url, wordOccurence, word, 2, 5, position);
					position++;
				}
			}

			// for title
			String title = doc.title();
			StringTokenizer titleTokens = new StringTokenizer(title);
			while (titleTokens.hasMoreTokens()) {
				String word = titleTokens.nextToken();
				helper(url, wordOccurence, word, 1, 10, position);
				position++;
			}

			// for body
			Element bodyData = doc.body();
			if (bodyData != null) {
				String body = bodyData.text();
				StringTokenizer bodyTokens = new StringTokenizer(body);

				while (bodyTokens.hasMoreTokens()) {
					String word = bodyTokens.nextToken();
					helper(url, wordOccurence, word, 3, 1, position);
					position++;
				}
			}

			// for anchor
			Elements links = doc.select("a");
			if (links != null) {
				int numOutLinks = links.size();

				for (Element link : links) {
					String anchor = link.text().trim();
					StringTokenizer anchorTokens = new StringTokenizer(anchor);
					String outLink = link.attr("href");

					// for page rank
					keyInfo.set("Link\t" + outLink);
					valueInfo.set(numOutLinks + "," + url);
					context.write(keyInfo, valueInfo);

					while (anchorTokens.hasMoreTokens()) {
						String word = anchorTokens.nextToken();
						helper(outLink, wordOccurence, word, 0, 10, 0);
					}
				}
			}

			for (String word : wordOccurence.keySet()) {
				keyInfo.set("Word\t" + word);
				String output = "";
				int size = wordOccurence.get(word).size();
				for (int i = 0; i < size; i++) {
					for (Occurence ocr : wordOccurence.get(word)) {
						output += "[" + ocr.toString() + "]\t";
					}
				}

				valueInfo.set(output);
				context.write(keyInfo, valueInfo);
			}

		}
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

			String result = "";
			for (Text val : values) {
				result += val.toString() + "\t";
			}
			valueInfo.set(result);
			// context.write(key, valueInfo);

			String keyWord = key.toString();
			if (keyWord.startsWith("Word")) {
				String realKey = keyWord.split("\t", 2)[1];
				String s = "";
				try {
					MessageDigest crypt = MessageDigest.getInstance("SHA-1");
					crypt.reset();
					crypt.update(realKey.getBytes("UTF-8"));
					s = byteArrayToHexString(crypt.digest());
				} catch (Exception e) {
					e.printStackTrace();
				}

				BigInteger keyValue = new BigInteger(s, 16);
				int n = (keyValue.divide(unit)).intValue();
				mos.write("output" + n, realKey, valueInfo);
			} else {
				String realKey = keyWord.split("\t", 2)[1];
				mos.write("links", realKey, valueInfo);
			}
		}

		private static String byteArrayToHexString(byte[] b) {
			String result = "";
			for (int i = 0; i < b.length; i++) {
				result += Integer.toString((b[i] & 0xff) + 0x100, 16)
						.substring(1);
			}
			return result;
		}
	}

	private static void helper(String url,
			HashMap<String, ArrayList<Occurence>> wordOccurence, String word,
			int type, int importance, int position) {

		int isCapital = 0;

		if (isAllLetter(word)) {
			if (isAllCapital(word)) {
				isCapital = 1;
			}
			word = Stemmer.getString(word.toLowerCase().toString());
		}

		if (!wordOccurence.containsKey(word)) {
			ArrayList<Occurence> tempList = new ArrayList<Occurence>();
			tempList.add(new Occurence(url, isCapital, type, importance,
					position));
			wordOccurence.put(word, tempList);
		} else {
			wordOccurence.get(word).add(
					new Occurence(url, isCapital, type, importance, position));
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
