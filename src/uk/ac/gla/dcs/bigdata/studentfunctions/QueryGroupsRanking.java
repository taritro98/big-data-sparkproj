package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;

/**
 * Performs reverse order sorting, filters out documents with high similarity score and computes the top 10 ranked
 * documents for each news article by taking input as query-wise grouped news articles
 * 
 * @author haet9
 *
 */
public class QueryGroupsRanking implements FlatMapGroupsFunction<Query, NewsArticleDPHScore, DocumentRanking> {

	private static final long serialVersionUID = 7551642684104219437L;
	int flag; // Flag variable
	
	@Override
	public Iterator<DocumentRanking> call(Query key, Iterator<NewsArticleDPHScore> values) throws Exception {

		List<NewsArticleDPHScore> newsArticleDPHScoreList = new ArrayList<>(); // List of NewsArticleDPHScore object
		List<NewsArticleDPHScore> newsArticleDPHScoreListFiltered = new ArrayList<>(); // List of Filtered NewsArticleDPHScore object

		List<RankedResult> rankedResultsList = new LinkedList<RankedResult>(); // List of ranked result
		List<DocumentRanking> documentRankingList = new LinkedList<DocumentRanking>(); // List of document ranking 

		// Stores NewsArticleDPHScore object into list 
		while (values.hasNext()) {
			NewsArticleDPHScore score = values.next();
			newsArticleDPHScoreList.add(score);
		}

		// Sorts the list by descending order of DPHScore
		Collections.sort(newsArticleDPHScoreList);
		Collections.reverse(newsArticleDPHScoreList);

        // Appends the highest ranked DPHScore document in the filtered List
		newsArticleDPHScoreListFiltered.add(newsArticleDPHScoreList.get(0));
		RankedResult rankedResult  = new RankedResult(newsArticleDPHScoreList.get(0).getId(), newsArticleDPHScoreList.get(0).getArticle(), newsArticleDPHScoreList.get(0).getDphScore());
		rankedResultsList.add(rankedResult);
		
		/* Calculates the normalised distance between titles of 2 documents and filters out one of the documents with
		 * similarity score lesser than 0.5 and keeps the document with the higher DPH score until top 10 ranked 
		 * documents are obtained. Then it appends documents into RankedResult structure.
		 *  */
		NewsArticleDPHScore next = null;
		outerloop:
		for(int i = 0; i < newsArticleDPHScoreList.size() -1 ; i++) {
			flag =0;
			NewsArticleDPHScore current = newsArticleDPHScoreList.get(i);

			String currentString = String.join("",current.getTitle());

			for(int j = 0; j < newsArticleDPHScoreListFiltered.size(); j++) {
				next = newsArticleDPHScoreListFiltered.get(j);
				String nextString = String.join("",next.getTitle());

				double similarityScore = TextDistanceCalculator.similarity(currentString, nextString);

				if(similarityScore < 0.5) {
					
					flag =1;
					break;
					
				}
			}
			
			if(flag ==0) {
				newsArticleDPHScoreListFiltered.add(current);
				RankedResult rankedResultCurrent  = new RankedResult(current.getId(), current.getArticle(), current.getDphScore());
				rankedResultsList.add(rankedResultCurrent);
				if(newsArticleDPHScoreListFiltered.size() > 9) {
					break outerloop;
				}
			}
		}

		DocumentRanking documentRanks = new DocumentRanking(key, rankedResultsList);
		documentRankingList.add(documentRanks);

		return documentRankingList.iterator();

	}

}
