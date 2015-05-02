package pageRankDB;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

@Entity
public class PageRankOccurence {
	@PrimaryKey
	private String url;
	private double pageRank;
	
	public String getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url = url;
	}
	
	public double getPageRank() {
		return pageRank;
	}
	
	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}
	
	public String toString() {
		return url + " " + pageRank;
	}
	
	
	
}
