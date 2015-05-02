package indexDB;

import static org.junit.Assert.*;

import org.junit.Test;

public class IndexDBWrapperTest {

	@Test
	public void testIndexDBWrapper() {
//		IndexDBWrapper db = new IndexDBWrapper("/home/joseph/Desktop/wordIndex");
//		db.setup();
//		System.out.println(db.getWordIndex("zurich"));
//		System.out.println(db.getWordIndex("zv"));
//		System.out.println(db.getWordIndex("yep"));
//		System.out.println(db.getWordIndex("yontheroad"));
//
//		db.close();
		
		String a = "在";
		System.out.println(isAllLetter(a));
		

	}

	public static boolean isAllLetter(String s) {
		for (int i = 0; i < s.length(); i++) {
			if (!Character.isLetter(s.charAt(i)))
				return false;
		}
		return true;
	}

}
