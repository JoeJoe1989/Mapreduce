package indexDB;

import java.util.ArrayList;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class WordOccurence {
	@PrimaryKey
	private String word;
	private double idf;
	private ArrayList<UrlOccurence> urlOccurences = new ArrayList<UrlOccurence>();
	
	public String getWord() {
		return word;
	}
	
	public void setWord(String word) {
		this.word = word;
	}
	public double getIdf() {
		return idf;
	}
	public void setIdf(double idf) {
		this.idf = idf;
	}
	public ArrayList<UrlOccurence> getUrlOccurences() {
		return urlOccurences;
	}
	public void add(UrlOccurence a) {
		urlOccurences.add(a);
	}
	
	public String toString(){
		return word + "\t" + idf + "\t" + urlOccurences;
	}
}
