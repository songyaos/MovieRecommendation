package org.CMPT732A3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Q3: Use HTable rating as data source
 * Compute the average rating per movie
 * @author songyao
 *
 */
public class ExtractMeanRating {

	final static String DEFAULT_OUTPUT = "mean_rating_output";
	final static String CONF_COLFAMILY = "conf.columnFamily";
	final static String CONF_QUALIFIER = "conf.qualifier";
	
	public enum Counter {ROWS};
	
	/**
	 * Get rows of data from HTable as data source
	 * emits <movieID, rating> pairs
	 * @author songyao
	 *
	 */
	static class RatingMapper extends TableMapper<IntWritable, IntWritable>{
		
		private byte[] columnFamily;
		private byte[] qualifier;
		@Override
		protected void setup(Context context) 
				throws IOException ,InterruptedException {
			// get column family and qualifer from context.getConfiguration()
			columnFamily = Bytes.toBytes(context.getConfiguration().get(CONF_COLFAMILY));
			qualifier = Bytes.toBytes(context.getConfiguration().get(CONF_QUALIFIER));
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
			int movieId, rating;
			/* HTable row is constructed as
			 * userId::movieId    column=rating:score, timestamp=xxxxxxxxxxxxx, value=score
			 */
			String rowId = Bytes.toStringBinary(row.get()); 
			ratingInfo = rowId.split("::");
			movieId = Integer.parseInt(ratingInfo[1]);
			//Get values from all columns
			
			for(KeyValue kv : columns.list()){
				score = Bytes.toStringBinary(kv.getValue());
				rating = Integer.parseInt(score);
				context.write(new IntWritable(movieId), new IntWritable(rating));
			}
			
		}
	}
	
	/**
	 * Calculate the average rating for a movie
	 * @author songyao
	 *
	 */
	static class RatingReducer extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable>{
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, 
				Context context) throws IOException ,InterruptedException {
			double sum = 0;
			int count = 0;
			for(IntWritable value : values){
				sum += (double)value.get();
				count++;
			}
			context.write(key, new DoubleWritable(sum/count));
			
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		String table, output, columnFamily, qualifier;
		if (args.length == 0){
			table = ConstructRatingTable.RATING_TABLE;
			output = DEFAULT_OUTPUT;
			columnFamily = ConstructRatingTable.COLFAM_RATING_INFO;
			qualifier = ConstructRatingTable.COL_RATING_SCORE;
			
		}else{
			table = args[0];
			output = args[1];
			columnFamily = args[2];
			qualifier = args[3];
		}
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", 
				"rcg-hadoop-01.rcg.sfu.ca,rcg-hadoop-02.rcg.sfu.ca,rcg-hadoop-03.rcg.sfu.ca");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		System.out.println("DEBUG: Configuration finished");
		// we can pass column family and qualifier in conf to reducers 
		conf.set(CONF_COLFAMILY, columnFamily);
		conf.set(CONF_QUALIFIER, qualifier);
		
		Scan scan = new Scan();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "parse table "+table);
		job.setJarByClass(ExtractMeanRating.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.out.println("DEBUG: Input Output job setup");
		//Set up the table mapper phase, and reducer phase
		TableMapReduceUtil.initTableMapperJob(table, scan, RatingMapper.class, 
									IntWritable.class, IntWritable.class, job);
		job.setReducerClass(RatingReducer.class);
		
		System.out.println("DEBUG: Map Reducer Setup Completed");
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
				
		job.waitForCompletion(true);
		
		
		
	}

}
