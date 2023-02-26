package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

/**
 *  Computes the average query-wise DPH Score by using the processed news articles from the NewsPreprocessor function. Uses a custom structure NewsArticleDPHScore to represent and store the DPH scores by query along with 
 *  other news article information and append into a list to be returned as an iterator.
 * @author haet9
 *
 */
public class NewsArticleDPHProcessor implements FlatMapFunction<NewsArticleProcessed,NewsArticleDPHScore>{

	private static final long serialVersionUID = 4688429466094545724L;
	
	Broadcast<List<Query>> queries; // Broadcasted list of Query objects
	LongAccumulator totalDocumentLength; // Length of all documents 
	Long totalCorpusDocuments; // Number of documents in Corpus
	double averageDocumentLengthInCorpus; // Average document length in corpus
	Map<String, Integer> totalQueryTermFrequency ; // Each query terms and its frequency in the corpus
	Map<String, Integer> queryTermFreqAccumulatorMap; // Stores pair of Query terms and its frequency present in all documents
	Integer totalDocumentLengthComputed; // Total terms in the corpus

	// Parameterized and Non-Parameterized constructors 
	public NewsArticleDPHProcessor() {}

	public NewsArticleDPHProcessor(Broadcast<List<Query>> queries, Integer totalDocumentLengthComputed, Long totalCorpusDocuments,
			Map<String, Integer> queryTermFreqAccumulatorMap) {
		this.queries = queries;
		this.totalCorpusDocuments = totalCorpusDocuments;
		this.totalDocumentLengthComputed = totalDocumentLengthComputed;
		this.queryTermFreqAccumulatorMap = queryTermFreqAccumulatorMap;
	}
    
	/*
	 * Calculates query-wise DPH Score for each document and filters out document with zero DPH Score
	 * */
	@Override
	public Iterator<NewsArticleDPHScore> call(NewsArticleProcessed value) throws Exception {

		totalQueryTermFrequency = queryTermFreqAccumulatorMap; // Pair of query terms and its frequency in the corpus

		averageDocumentLengthInCorpus = totalDocumentLengthComputed*1.0/totalCorpusDocuments; // Calculates average document length in corpus

		List<NewsArticleDPHScore> newsArticleDPHScoreList = new ArrayList<NewsArticleDPHScore>(); // Empty ArrayList for adding not null dphscore				

		for (int i=0; i<queries.getValue().size(); i++) {
			
			Query query = queries.getValue().get(i);			 
			String id = value.getId();
			List<String> title = value.getTitle();
			List<String> contents = value.getContents();
			
			// Calculates the current document length by adding the size of title and contents
			int currentDocumentLength = contents.size() + title.size();

			// Stores the title and content in the one list to calculate the occurrence of each query terms in the current document
			List<String> document = new ArrayList<>();
			document.addAll(title);
			document.addAll(contents);

			Double avgDPHScore = 0.0;
			List<Double> termDPHScoreList = new ArrayList<Double>();

			for (String queryTerm: query.getQueryTerms()) {

				short termFreqinDocument = (short) Collections.frequency(document, queryTerm);

				int totalTermFrequencyInCorpus = totalQueryTermFrequency.get(queryTerm).intValue();

				double termDPHScore = DPHScorer.getDPHScore(
						termFreqinDocument,
						totalTermFrequencyInCorpus,
						currentDocumentLength,
						averageDocumentLengthInCorpus,
						totalCorpusDocuments);

				// Replace the NAN value of DPHScore to double 0.0
				if(Double.isNaN(termDPHScore))
				{
					termDPHScore = 0.0;
					termDPHScoreList.add(termDPHScore);
				}
				else
				{
					termDPHScoreList.add(termDPHScore);
				}
			}
			avgDPHScore = termDPHScoreList.stream().mapToDouble(d -> d).average().orElse(0.0);

			if(avgDPHScore > 0.0)
			{
				NewsArticleDPHScore newsDPHScores = new NewsArticleDPHScore(query, id, title, avgDPHScore, value.getArticle());
				newsArticleDPHScoreList.add(newsDPHScores);
			}
		}
		return newsArticleDPHScoreList.iterator();
	}		
}
