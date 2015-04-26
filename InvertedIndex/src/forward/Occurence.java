package forward;

public class Occurence {

	public String url;

	// 0 for non-capital 1 for capital
	public int capital;
	// 0 for anchor, 1 for title, 2 for meta, 3 for body
	public int type;
	// 10, 10, 5, 1
	public int importance;
	public int position;

	public Occurence(String url, int capital, int type,
			int position) {
		this.url = url;
		this.capital = capital;
		this.type = type;
		this.position = position;
		switch(type) {
		case 0: 
		case 1: importance = 10;
		break;
		case 2: importance = 5;
		break;
		case 3: importance = 1;
		break;
		}
		if (capital == 1) importance += 2;
	}

	@Override
	public String toString() {
		return url + "\t" + type + "\t" + importance + "\t" + position;
	}

}
