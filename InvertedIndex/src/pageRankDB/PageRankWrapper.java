package pageRankDB;

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

public class PageRankWrapper {
	private String direc;
	private static String envDirectory = null;
	private static Environment myEnv;
	private static EntityStore store;
	PrimaryIndex<String, PageRankOccurence> pageIndex;
	
	public PageRankWrapper(String direc) {
		this.direc = direc;
	}


	public void setup() {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		StoreConfig storeConfig = new StoreConfig();

		envConfig.setAllowCreate(true);
		storeConfig.setAllowCreate(true);

		myEnv = new Environment(new File(direc), envConfig);
		store = new EntityStore(myEnv, "EntityStore", storeConfig);
		
		pageIndex = store.getPrimaryIndex(String.class, PageRankOccurence.class);
	}
	
	public void putPage(PageRankOccurence a) {
		pageIndex.put(a);
	}
	
	public PageRankOccurence getPage(String word) {
		return pageIndex.get(word);
	}
	
	public void close() {
		if (store != null) store.close();
		if (myEnv != null) myEnv.close();
	}
	
	public static void main(String[] args) throws IOException {
		PageRankWrapper db = new PageRankWrapper("/home/joseph/Desktop/pageRank");
		
		db.setup();
		File file = new File("/home/joseph/Desktop/page");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		while ((line = br.readLine()) != null) {
			String[] strs = line.split("\\s+");
			String url = strs[0];
			double pageRank = Double.parseDouble(strs[1].split(",")[1]);
			PageRankOccurence page = new PageRankOccurence();
			page.setUrl(url);
			page.setPageRank(pageRank);
			db.putPage(page);
			System.out.println(page);
		}
		db.close();
	}

}

