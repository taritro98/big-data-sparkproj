package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * 
 * Initializes LongAccumulators for each term in query and returns a HashMap of query term and an accumulator
 * for storing the query term frequency counts as key-value pair.
 * 
 * @author haet9
 *
 */
public class QueryTermAccumulatorFunction {

	Broadcast<List<Query>> queries; // Broadcasted List of Query objects
	List<String> queryTermList; // List of Query terms after tokenization, stopword removal and stemming
	public static Map<String, LongAccumulator> queryTermFreqAccumulatorMap = new HashMap<>(); // Hashmap structure for storing the query terms and term frequency accumulator.
	public long termFreq=0; // Frequency of the query terms
	SparkSession spark; // Spark session object

	// Parameterized constructor for initializing the class variables and obtain a unique list of query terms
	public QueryTermAccumulatorFunction(Broadcast<List<Query>> queries, SparkSession spark) {
		super();
		this.queries = queries;
		this.queryTermList = convertSetToList();
		this.spark = spark;

	}
	
    // Obtain a unique set of query terms and convert into list
	private List<String> convertSetToList()
	{
		Set<String> queryTermSet = new HashSet<>();
		for (Query query: this.queries.getValue()) {
			Set<String> querySet = new HashSet<>(query.getQueryTerms());
			queryTermSet.addAll(querySet);

		}
		List<String> queryTermLists = new ArrayList<>(queryTermSet);
		return queryTermLists;
	}

	// Initializes a map with an accumulator for each query term as a key-value pair
	public Map<String, LongAccumulator> getAccumulatorMap(){

		for(String queryTerm: queryTermList) {

			queryTermFreqAccumulatorMap.put(queryTerm, spark.sparkContext().longAccumulator());

		}
		return queryTermFreqAccumulatorMap;
	}

}
