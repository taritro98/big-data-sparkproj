package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleDPHScore;

/**
 * Returns the Query object for each NewsArticleDPHScore object using a map transformation
 * 
 * @author haet9
 *
 */
public class QueryKeyFunction implements MapFunction<NewsArticleDPHScore,Query>{

	private static final long serialVersionUID = 2355774322412688215L;

	@Override
	public Query call(NewsArticleDPHScore value) throws Exception {

		return value.getQuery();

	}





}
