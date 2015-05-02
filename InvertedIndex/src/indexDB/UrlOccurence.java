package indexDB;

import java.util.ArrayList;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;

@Persistent
public class UrlOccurence {
	private String url;
	private double tf;
	private ArrayList<Integer> positions = new ArrayList<Integer>();
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
	public double getTF() {
		return tf;
	}
	public void setTF(double tf) {
		this.tf = tf;
	}
	public ArrayList<Integer> getPositions() {
		return positions;
	}
	public void add(int a) {
		positions.add(a);
	}

	public String toString() {
		return url + " " + tf + " " + positions;
	}
	
	
	
}
