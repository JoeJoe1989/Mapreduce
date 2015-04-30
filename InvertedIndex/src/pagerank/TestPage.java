package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestPage extends Configured implements Tool {
	String basePath;

	public TestPage(String basePath) {
		this.basePath = basePath;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TestPage(""), args);
	}

	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {

		// Iterate PageRank.

		double current = 0;
		double previous = 200;
		while (Math.abs(current - previous) > 100) {
			previous = current;

			current = iteratePageRank();

		}

		return 0;

	}

	private void oneRound(int i) throws IOException {
		   Job job = Job.getInstance(getConf());  
	        job.setJobName("PageRank:Basic:iteration" + i);  
	        job.setJarByClass(TestPage.class);
	        String in = basePath + "/iter" + i;  
	        int temp = i + 1;
	        String out = basePath + "/iter" + temp;
	        
	        
		
		
	}

	// Run each iteration.
	private void iteratePageRank(int i) throws Exception {
		oneRound(i);
		double deviation = computeDeviation();
		return deviation;

	}

}
