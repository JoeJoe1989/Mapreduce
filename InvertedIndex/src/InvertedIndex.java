import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class InvertedIndex {
	public static class Map extends
			Mapper<NullWritable, BytesWritable, Text, Text> {

		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			Text keyInfo = new Text();
			Text valueInfo = new Text();
			
			Path filePath = ((FileSplit) context.getInputSplit()).getPath();
			String fileNameString = filePath.getName();

			byte[] fileContentByte = value.getBytes();
			
			
			String html = new String(fileContentByte);
			Document doc = Jsoup.parse(html);

			String body = doc.body().text();
			StringTokenizer tokens = new StringTokenizer(body);
			
			int position = 0;
			
			while(tokens.hasMoreTokens()){
				keyInfo.set(tokens.nextToken());
				valueInfo.set(position + "::" + fileNameString);
				context.write(keyInfo, valueInfo);
				position++;
			}

		}	
	}
	
	private boolean isAllCapital(String s) {
		for (int i = 0; i < s.length(); i++) {
			if (! Character.isUpperCase(s.charAt(i))) return false;
		}
		return true;
	}
	
	private boolean isAllLetter(String s) {
		for (int i = 0; i < s.length(); i++) {
			if (! Character.isLetter(s.charAt(i))) return false;
		}
		return true;
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// System.out.println("##################################################");
			String result = "";
			for (Text val : values) {
				result += val.toString() + "\t";
			}
			valueInfo.set(result);
			context.write(key, valueInfo);
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

//		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean ret = job.waitForCompletion(true);
		if (!ret) {
			throw new Exception("Job Failed");
		}

	}
}
