package inverted;

public class Occurence {

	public String url;

	// 0 for non-capital 1 for capital
	public int capital;
	// 0 for anchor, 1 for title, 2 for meta, 3 for title
	public int type;
	// 10, 10, 5, 1
	public int importance;
	public int position;

	public Occurence(String url, int capital, int type, int importance,
			int position) {
		this.url = url;
		this.capital = capital;
		this.type = type;
		this.importance = importance;
		this.position = position;
	}

	@Override
	public String toString() {
		return url + "\t" + type + "\t" + importance + "\t" + position;

	}

}
