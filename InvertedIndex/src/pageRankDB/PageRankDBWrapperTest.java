package pageRankDB;

import static org.junit.Assert.*;

import org.junit.Test;

public class PageRankDBWrapperTest {

	@Test
	public void testIndexDBWrapper() {
		PageRankWrapper db = new PageRankWrapper("/home/joseph/Desktop/pageRank");
		db.setup();
		System.out.println(db.getPage("https%3A%2F%2Fwww.twitter.com%2FYahooMusic").getPageRank());
		System.out.println(db.getPage("https%3A%2F%2Fwww.twitter.com%2FYahooBeauty").getPageRank());

		db.close();
		

	}

}
