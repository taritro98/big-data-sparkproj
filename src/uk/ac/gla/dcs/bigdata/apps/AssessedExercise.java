package uk.ac.gla.dcs.bigdata.apps;

import static org.apache.spark.sql.functions.col;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleDPHProcessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsPreprocessor;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryGroupsRanking;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryKeyFunction;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryTermAccumulatorFunction;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleProcessed;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[4]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		//if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // default is a sample of 5000 news articles

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}
		
		
	}
	
	
	
	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------
		
		// Broadcasted list of Query objects
		Broadcast<List<Query>> query = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queries.collectAsList());
		
		// Filtering news articles for null values in id, title, contents and subtype columns
		Dataset<NewsArticle> filteredData = news.filter(col("id").isNotNull().and(col("title").isNotNull().and(col("contents").isNotNull().and(col("contents.subtype").isNotNull().and(col("contents.content").isNotNull())))));
		
		// Declaring accumulators to compute parameters for DPH score
		LongAccumulator totalDocumentLength = spark.sparkContext().longAccumulator();
		LongAccumulator totalCorpusDocuments = spark.sparkContext().longAccumulator();
		Long totalDocs = filteredData.count();
		
		// HashMap accumulator for defining and calculating query-term wise frequency in corpus
		QueryTermAccumulatorFunction queryTermFreqFunc = new QueryTermAccumulatorFunction(query, spark);
		Map<String, LongAccumulator> queryTermFreqAccumulatorMap = queryTermFreqFunc.getAccumulatorMap();
		
		// Spark transformation 1 - Map function for processing (tokenizing, stemming) news articles and precomputing DPH score parameters
		NewsPreprocessor newsArticle = new NewsPreprocessor(totalDocumentLength, totalCorpusDocuments, query, queryTermFreqAccumulatorMap);
		Dataset<NewsArticleProcessed> newsArticleProcessed = filteredData.map(newsArticle, Encoders.bean(NewsArticleProcessed.class)); // this converts each row into a NewsArticle
		
		// Spark action for executing transformation function and obtaining DPH score parameters
		newsArticleProcessed.count();
		
		// Obtains query term-wise frequency and total document length from accumulators
		Map<String, Integer> queryTermCountsComputed = new HashMap<>();
		Integer totalDocumentLengthComputed = totalDocumentLength.value().intValue();

		for (java.util.Map.Entry<String, LongAccumulator> entry : queryTermFreqAccumulatorMap.entrySet()) {
			queryTermCountsComputed.put(entry.getKey(),Math.toIntExact(entry.getValue().value()));
		}
		
		// Spark transformation 2 - FlatMap function to calculate average DPH score per query for each document 
		NewsArticleDPHProcessor newsDPHScore = new NewsArticleDPHProcessor(query, totalDocumentLengthComputed, totalDocs, queryTermCountsComputed);
		Dataset<NewsArticleDPHScore> newsArticleDPH = newsArticleProcessed.flatMap(newsDPHScore, Encoders.bean(NewsArticleDPHScore.class)); 

		// Grouping function to group NewsArticleDPHScore custom objects by query
		QueryKeyFunction queryKey = new QueryKeyFunction();
		KeyValueGroupedDataset<Query, NewsArticleDPHScore> queryGrouped = newsArticleDPH.groupByKey(queryKey, Encoders.bean(Query.class));
		
		// Spark transformation 3 - FlatMap groups function to sort documents by descending order, filter out similar documents and fetch top 10 ranked documents per query
		QueryGroupsRanking queryGroupRanking = new QueryGroupsRanking();
		Dataset<DocumentRanking> newsArticleDPHResult = queryGrouped.flatMapGroups(queryGroupRanking, Encoders.bean(DocumentRanking.class));
		
		//Spark action to fetch the top 10 ranked documents
		List<DocumentRanking> docRankingList = newsArticleDPHResult.collectAsList();
		
		//System.out.println("Doc Ranking -> "+docRankingList);
		return docRankingList; // replace this with the the list of DocumentRanking output by your topology
	}
	
	
}
