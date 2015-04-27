package forward;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;

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
	public static class Map extends
			Mapper<NullWritable, BytesWritable, Text, Text> {

		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			HashMap<String, ArrayList<Occurence>> wordOccurence = new HashMap<String, ArrayList<Occurence>>();

			Text keyInfo = new Text();
			Text valueInfo = new Text();

			Path filePath = ((FileSplit) context.getInputSplit()).getPath();
			String url = filePath.getName();

			url = URLDecoder.decode(url, "UTF-8");
			String hostName = new URL(url).getHost();

			byte[] fileContentByte = value.getBytes();

			String html = new String(fileContentByte);
			Document doc = Jsoup.parse(html, url);

			int position = 1;

			Elements metaData = doc.select("meta[name]");
			if (metaData != null) {
				String metaContent = "";
				for (Element data : metaData) {
					if ("keywords".equals(data.attr("name"))
							|| "description".equals(data.attr("name")))
						metaContent += data.attr("content") + " ";
				}

				if (!"".equals(metaContent)) {
					String[] metaTokens = metaContent.split("[^a-zA-Z0-9]+");
					for (String word : metaTokens) {
						helper(url, wordOccurence, word, 2, position);
						position++;
					}
				}

			}

			// for title
			String title = doc.title();

			if (!"".equals(title)) {
				String[] titleTokens = title.split("[^a-zA-Z0-9]+");
				for (String word : titleTokens) {
					helper(url, wordOccurence, word, 1, position);
					position++;
				}
			}

			// for body
			Element bodyData = doc.body();
			if (bodyData != null) {
				String body = bodyData.text();

				if (!"".equals(body)) {
					String[] bodyTokens = body.split("[^a-zA-Z0-9]+");
					for (String word : bodyTokens) {
						helper(url, wordOccurence, word, 3, position);
						position++;
					}

				}

			}

			// for anchor
			Elements links = doc.select("a[href]");
			if (links != null) {
				int numOutLinks = 0;
				ArrayList<String> outLinks = new ArrayList<String>();
				for (Element link : links) {
					String anchor = link.text().trim();
					String outLink = link.attr("abs:href");
					try {
						String outHostName = new URL(outLink).getHost();
						if (!hostName.equals(outHostName)) {
							numOutLinks++;
							outLinks.add(outLink);
//							keyInfo.set("Link\t" + outHostName);
//							valueInfo.set(numOutLinks + "," + hostName);
//							context.write(keyInfo, valueInfo);
						}
					} catch (Exception e) {
					}

					if (!"".equals(anchor)) {
						// for page rank
						String[] anchorTokens = anchor.split("[^a-zA-Z0-9]+");
						for (String word : anchorTokens) {
							helper(outLink, wordOccurence, word, 0, 0);
							position++;
						}

					}

				}
				
				for (String outLink: outLinks) {
					keyInfo.set("Link\t" + outLink);
					valueInfo.set(numOutLinks + "," + url);
					context.write(keyInfo, valueInfo);
				}
			}

			for (String word : wordOccurence.keySet()) {
				for (Occurence ocr : wordOccurence.get(word)) {
					keyInfo.set("Url\t" + ocr.url);
					String output = word + "," + ocr.importance + ","
							+ ocr.position;
					valueInfo.set(output);
					context.write(keyInfo, valueInfo);
				}

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

	private static void helper(String url,
			HashMap<String, ArrayList<Occurence>> wordOccurence, String word,
			int type, int position) {

		int isCapital = 0;
		word = word.replaceAll("[^A-Za-z0-9]*$|^[^A-Za-z0-9]*", "");
		if (isAllLetter(word)) {
			if (isAllCapital(word)) {
				isCapital = 1;
			}
			word = Stemmer.getString(word);
		}

		word = word.toLowerCase().toString();

		if (!wordOccurence.containsKey(word)) {
			ArrayList<Occurence> tempList = new ArrayList<Occurence>();
			tempList.add(new Occurence(url, isCapital, type, position));
			wordOccurence.put(word, tempList);
		} else {
			wordOccurence.get(word).add(
					new Occurence(url, isCapital, type, position));
		}

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
			String url = key.toString().split("\t", 2)[1];
			if (key.toString().startsWith("Link")) {
				StringBuilder sb = new StringBuilder();
				HashMap<String, Integer> hm = new HashMap<String, Integer>();
				for (Text value : values) {
					String str = value.toString();
					if (hm.containsKey(str)) {
						hm.put(str, hm.get(str) + 1);
					} else {
						hm.put(str, 1);
					}
				}
				
				for (String str: hm.keySet()) {
					sb.append(str + "," + hm.get(str) + "||");
				}
				keyInfo.set(url);
				valueInfo.set(sb.toString());
				mos.write("links", keyInfo, valueInfo);

			}

			if (key.toString().startsWith("Url")) {
				HashMap<String, CombinedOccurence> hm = new HashMap<String, CombinedOccurence>();
				for (Text value : values) {
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
				for (String word : hm.keySet()) {
					max = Math.max(max, hm.get(word).tf);
				}
				for (String word : hm.keySet()) {
					CombinedOccurence temp = hm.get(word);
					temp.tf = 0.5 + 0.5 * temp.tf / max;
					keyInfo.set(word);
					valueInfo.set(temp.toString());

					context.write(keyInfo, valueInfo);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "ForwardIndex");

		job.setJarByClass(ForwardIndex.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		MultipleOutputs.addNamedOutput(job, "links", TextOutputFormat.class,
				Text.class, Text.class);

		boolean ret = job.waitForCompletion(true);
		if (!ret) {
			throw new Exception("Job Failed");
		}

	}
}
