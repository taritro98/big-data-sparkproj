package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

/**
 * Preprocessed NewsArticles by stemming, tokenization and filtering null values from documents based on contents and subtype; also computes the query term
 * frequency, total document length and increments the respective accumulators
 * @author haet9
 *
 */
public class NewsPreprocessor implements MapFunction<NewsArticle,NewsArticleProcessed>{

	private static final long serialVersionUID = -6952801201940912354L;

	private transient TextPreProcessor processor; // Transient TextPreProcessor object
	LongAccumulator totalDocumentLength; // Length of all documents 
	LongAccumulator totalCorpusDocuments; // Number of documents in Corpus
	Map<String, LongAccumulator> queryTermFreqAccumulatorMap; // Stores pair of Query terms and its frequency present in all documents
	Broadcast<List<Query>> queries; // Broadcasted list of query objects
	List<String> queryTermList; // List of Query Terms
	public long termFreq=0; // Frequency of the query terms
	public NewsArticle article; // Object of NewsArticle class

	//Parameterized and Non-Parameterized constructors 
	public NewsPreprocessor() {}

	public NewsPreprocessor(LongAccumulator totalDocumentLength,LongAccumulator totalCorpusDocuments, 
			Broadcast<List<Query>> queries, Map<String, LongAccumulator> queryTermFreqAccumulatorMap) {
		super();
		this.totalDocumentLength = totalDocumentLength;
		this.totalCorpusDocuments = totalCorpusDocuments;
		this.queries = queries;
		this.queryTermList = convertSetToList();
		this.queryTermFreqAccumulatorMap = queryTermFreqAccumulatorMap;
	}

	// Obtain a unique set of query terms and convert into list
	private List<String> convertSetToList()
	{
		Set<String> queryTermSet = new HashSet<>();
		for (Query query: this.queries.getValue()) {
			Set<String> querySet = new HashSet<>(query.getQueryTerms());
			queryTermSet.addAll(querySet);

		}
		List<String> queryTermList = new ArrayList<>(queryTermSet);
		return queryTermList;
	}

	// Calculates the frequency of all the query terms from each document
	public Map<String, LongAccumulator> queryTermFreq(List<String> titleProcessed, List<String> contentProcessed) throws Exception {
		List<String> document = new ArrayList<>();
		document.addAll(titleProcessed);
		document.addAll(contentProcessed);

		for(String queryTerm: queryTermList) {
			termFreq = Collections.frequency(document, queryTerm);

			queryTermFreqAccumulatorMap.get(queryTerm).add(termFreq);
		}
		return queryTermFreqAccumulatorMap;
	}

   
	/*
	 * The call function verifies that the contents object and subtype field is not null. It also checks that the subtype field should be 
	 * paragraph and the contents should be 5 paragraph long. Further, the title and the contents of each document is tokenized, stemmed and 
	 * stopwords are removed. Then the total length of each document is calculated. 
	 * */
	@Override
	public NewsArticleProcessed call(NewsArticle value) throws Exception {

		if (processor==null) processor = new TextPreProcessor();

		String uniqueId = null;
		List<String> titleProcessed = new ArrayList<String>();
		List<String> contentProcessed = new ArrayList<String>();

		uniqueId = (value.getId());
		titleProcessed = processor.process(value.getTitle());

		List<ContentItem> contents = value.getContents();
		
		// Initialized counter
		int count = 0;
		if(!contents.isEmpty() && contents !=null)
		{
			for (ContentItem items: contents) {
				if( items !=null) {
					if (items.getSubtype() != null) {
						String subtype = items.getSubtype();
						if (subtype != null && !subtype.isEmpty() && subtype.equals("paragraph") && count < 5) {
							// Appended processed content list
							contentProcessed.addAll(processor.process(items.getContent()));
							count = count + 1;
						}
					}
				}
			}
		}
		queryTermFreqAccumulatorMap = queryTermFreq(titleProcessed, contentProcessed);
		
		article = value;
		
		NewsArticleProcessed newsArticle = new NewsArticleProcessed(uniqueId, titleProcessed, contentProcessed, article);

		totalDocumentLength.add(titleProcessed.size() + contentProcessed.size());
		
		return newsArticle;


	}



}
