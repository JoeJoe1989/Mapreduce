package indexDB;

import static org.junit.Assert.*;

import org.junit.Test;

public class IndexDBWrapperTest {

	@Test
	public void testIndexDBWrapper() {
		IndexDBWrapper db = new IndexDBWrapper("/home/joseph/Desktop/wordIndex");
		db.setup();
		System.out.println(db.getWordIndex("z1").getUrlOccurences());
		System.out.println(db.getWordIndex("z7p").getUrlOccurences());
		System.out.println(db.getWordIndex("zimbabw").getUrlOccurences());
		db.close();
		

	}


}
