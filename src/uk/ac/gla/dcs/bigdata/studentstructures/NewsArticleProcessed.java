package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import java.io.Serializable;
import java.util.List;

/**
 * This structure is used to represent the selected documents' id, title, contents and article, 
 * after filtering null field documents on which the tokenization, stemming and stopwords removal is done.
 * 
 * @author haet9
 *
 */
public class NewsArticleProcessed implements Serializable {

	private static final long serialVersionUID = -1361193371758844533L;

	String id; // Unique document identifier
	List<String> title; // List of title terms of the document after tokenization, stopword removal and stemming
	List<String> contents; // List of content terms of the document after tokenization, stopword removal and stemming
	public NewsArticle article = new NewsArticle(); // Article object of the NewsArticle Class


	//Parameterized and Non-Parameterized Constructors 
	public NewsArticleProcessed() {}

	public NewsArticleProcessed(String id, List<String> title, List<String> contents, NewsArticle article) {
		super();
		this.id = id;
		this.title = title;
		this.contents = contents;
		this.article = article;

	}

	//Getters and Setters Methods
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

	public List<String> getContents() {
		return contents;
	}

	public void setContents(List<String> contents) {
		this.contents = contents;
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

}
