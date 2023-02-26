package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.List;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * This structure is used to represent the filtered documents' query, id, title, dphScore and article. It is also
 * responsible for comparing the documents based on DPH Score.
 * 
 * @author haet9
 *
 */
public class NewsArticleDPHScore implements Serializable, Comparable<NewsArticleDPHScore> {

	private static final long serialVersionUID = -7311464349415469277L;

	Query query; // The query object
	String id; // Unique document identifier
	List<String> title; // List of title terms of the document after tokenization, stopword removal and stemming 
	double dphScore; // DPH Score of the document with respect to each query
	NewsArticle article; // The article object

	// Parameterized and Non-Parameterized constructors
	public NewsArticleDPHScore() {}

	public NewsArticleDPHScore(Query query, String id, List<String> title, double dphScore, NewsArticle article) {
		super();
		this.query = query;
		this.id = id;
		this.title = title;
		this.dphScore = dphScore;
		this.article = article;
	}

	// Getters and Setters Methods
	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getTitle() {
		return title;
	}

	public void setTitle(List<String> title) {
		this.title = title;
	}

	public double getDphScore() {
		return dphScore;
	}

	public void setDphScore(double dphScore) {
		this.dphScore = dphScore;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	@SuppressWarnings("deprecation")
	@Override
	public int compareTo(NewsArticleDPHScore o) {
		return new Double(dphScore).compareTo(o.dphScore);
	}





}
