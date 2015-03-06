package org.CMPT732A3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * Q4: Use HTable rating as data source and data sink
 * Count the number of ratings per movie, write movieInfo table
 * @author songyao
 *
 */
public class ExtractRatingNumber {

	//final static String DEFAULT_OUTPUT = "rating_number_output";
	final static String CONF_RATING_COLFAMILY = "conf.ratingColumnFamily";
	final static String CONF_RATING_QUALIFIER_SCORE = "conf.ratingQualifierScore";
	final static String CONF_MOVIE_COLFAMILY = "conf.movieColumnFamily";
	final static String CONF_MOVIE_QUALIFIER_COUNT = "conf.movieQualifierCount";
	
	final static String NEW_MOVIE_QUALIFIER = "count";
	
	public enum Counter {ROWS};
	
	/**
	 * Get rows of data from HTable as data source
	 * emits <movieID, rating> pairs
	 * @author songyao
	 *
	 */
	static class RatingMapper extends TableMapper<Text, IntWritable>{
		
		private byte[] columnFamily;
		private byte[] qualifier;
		@Override
		protected void setup(Context context) 
				throws IOException ,InterruptedException {
			// get column family and qualifer from context.getConfiguration()
			columnFamily = Bytes.toBytes(context.getConfiguration().get(CONF_RATING_COLFAMILY));
			qualifier = Bytes.toBytes(context.getConfiguration().get(CONF_RATING_QUALIFIER_SCORE));
			System.out.println("DEBUG: Column Family: "+Bytes.toString(columnFamily)
					+" ,Qualifier: "+Bytes.toString(qualifier));
		}
		
		@SuppressWarnings("deprecation")
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context)
					throws IOException, InterruptedException{
			//Keep track the number of rows that has been processed
			context.getCounter(Counter.ROWS).increment(1);
			String[] ratingInfo;
			String score = null;
			String movieId;
			int rating;
			/* HTable row is constructed as
			 * userId::movieId    column=rating:score, timestamp=xxxxxxxxxxxxx, value=score
			 */
			String rowId = Bytes.toStringBinary(row.get()); 
			ratingInfo = rowId.split("::");
			movieId = ratingInfo[1];
			//Get values from all columns
			for(KeyValue kv : columns.list()){
				score = Bytes.toStringBinary(kv.getValue());
				rating = Integer.parseInt(score);
				context.write(new Text(movieId), new IntWritable(rating));
			}
			
		}
	}
	
	/**
	 * Get the total number of rating for a movie
	 * @author songyao
	 *
	 */
	static class RatingReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{
		
		private byte[] columnFamily;
		private byte[] qualifier;
		@Override
		protected void setup(Context context) 
				throws IOException ,InterruptedException {
			// get column family and qualifer from context.getConfiguration()
			columnFamily = Bytes.toBytes(context.getConfiguration().get(CONF_MOVIE_COLFAMILY));
			qualifier = Bytes.toBytes(context.getConfiguration().get(CONF_MOVIE_QUALIFIER_COUNT));
			System.out.println("DEBUG: Column Family: "+Bytes.toString(columnFamily)
					+" ,Qualifier: "+Bytes.toString(qualifier));
		}
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, 
				Context context) throws IOException ,InterruptedException {
			int count = 0;
			byte[] movieId;
			movieId = Bytes.toBytes(key.toString());
			for(@SuppressWarnings("unused") IntWritable value : values){
				count++;
			}
			// Add a new column to store the total number of rating
			Put put = new Put(movieId);
			put.add(columnFamily, qualifier, Bytes.toBytes(String.valueOf(count)));
			context.write(new ImmutableBytesWritable(movieId), put);
			
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String ratingTable, ratingColumnFamily, ratingQualifierScore;
		String movieTable, movieColumnFamily;
		if (args.length == 0){
			ratingTable = ConstructRatingTable.RATING_TABLE;
			ratingColumnFamily = ConstructRatingTable.COLFAM_RATING_INFO;
			ratingQualifierScore = ConstructRatingTable.COL_RATING_SCORE;
			movieTable = ConstructMovieTable.MOVIE_TABLE;
			movieColumnFamily = ConstructMovieTable.COLFAM_MOVIE_INFO;
			
		}else{
			ratingTable = args[0];
			movieTable = args[1];
			ratingColumnFamily = args[2];
			movieColumnFamily = args[3];
			ratingQualifierScore = args[4];
		}
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", 
				"rcg-hadoop-01.rcg.sfu.ca,rcg-hadoop-02.rcg.sfu.ca,rcg-hadoop-03.rcg.sfu.ca");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		System.out.println("DEBUG: Configuration finished");
		// we can pass column family and qualifier in conf to reducers 
		conf.set(CONF_RATING_COLFAMILY, ratingColumnFamily);
		conf.set(CONF_RATING_QUALIFIER_SCORE, ratingQualifierScore);
		conf.set(CONF_MOVIE_COLFAMILY, movieColumnFamily);
		conf.set(CONF_MOVIE_QUALIFIER_COUNT, NEW_MOVIE_QUALIFIER);
		
		Scan scan = new Scan();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "parse table "+ratingTable);
		job.setJarByClass(ExtractRatingNumber.class);
		
		System.out.println("DEBUG: Input Output job setup");
		//Set up the table mapper phase, and reducer phase
		TableMapReduceUtil.initTableMapperJob(ratingTable, scan, RatingMapper.class, 
									Text.class, IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(movieTable, 
									RatingReducer.class, job);
		
		System.out.println("DEBUG: Map Reducer Setup Completed");
				
		job.waitForCompletion(true);
		
		
		
	}

}
