package indexDB;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

public class IndexDBWrapper {
	private String direc;
	private static String envDirectory = null;
	private static Environment myEnv;
	private static EntityStore store;
	PrimaryIndex<String, WordOccurence> wordIndex;

	public IndexDBWrapper(String direc) {
		this.direc = direc;
	}

	public void setup() {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		StoreConfig storeConfig = new StoreConfig();

		envConfig.setAllowCreate(true);
		storeConfig.setAllowCreate(true);

		myEnv = new Environment(new File(direc), envConfig);
		store = new EntityStore(myEnv, "EntityStore", storeConfig);

		wordIndex = store.getPrimaryIndex(String.class, WordOccurence.class);
	}

	public void putWordIndex(WordOccurence a) {
		wordIndex.put(a);
	}

	public WordOccurence getWordIndex(String word) {
		return wordIndex.get(word);
	}

	public void close() {
		if (store != null)
			store.close();
		if (myEnv != null)
			myEnv.close();
	}

	public static void main(String[] args) throws IOException {
		final int numberOfUrls = 10000;
		IndexDBWrapper db = new IndexDBWrapper("/home/joseph/Desktop/wordIndex");

		db.setup();
		File file = new File("/home/joseph/Desktop/invert");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		while ((line = br.readLine()) != null) {
			String word = line.split("\t", 2)[0];
			String[] occurences = line.split("\t", 2)[1].split("\\|\\|");
			WordOccurence wordOccur = new WordOccurence();
			wordOccur.setWord(word);
			wordOccur.setIdf(Math.log10(numberOfUrls / occurences.length));
			for (String occurence : occurences) {
				// System.out.println(occurence);
				String[] strs = occurence.split("\\s+");
				String url = strs[0];
				double tf = Double.parseDouble(strs[1]);
				String[] nums = strs[2].split(",");
				UrlOccurence occur = new UrlOccurence();
				occur.setUrl(url);
				occur.setTF(tf);
				for (String num : nums) {
					int position = Integer.parseInt(num);
					occur.add(position);
				}
				wordOccur.add(occur);
			}
			db.putWordIndex(wordOccur);
			System.out.println(wordOccur);
		}
		db.close();
	}

}
