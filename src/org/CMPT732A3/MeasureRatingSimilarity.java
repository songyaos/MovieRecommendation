package org.CMPT732A3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.CMPT732A3.ExtractRatingNumber.RatingReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Q5: Measure the similarities of two movie ratings
 * @author songyao
 *
 */
public class MeasureRatingSimilarity {

	final static String DEFAULT_OUTPUT = "similarity_output";
	final static String FINAL_OUTPUT = "SIMILARITY_FINAL_OUTPUT";
	final static String CONF_COLFAMILY = "conf.columnFamily";
	final static String CONF_QUALIFIER = "conf.qualifier";
	final static String SIMI_Table = "songyaos_simitable";
	
	public enum Counter {ROWS};
	
	/**
	 * Get rows of data from HTable as data source
	 * emits <movieID, rating> pairs
	 * @author songyao
	 *
	 */
	static class RatingMapper1 extends TableMapper<Text, MovieRatingWritable>{
		
		private byte[] columnFamily;
		private byte[] qualifier;
		
		private MovieRatingWritable mr = new MovieRatingWritable();
		
		private String[] ratingInfo;
		private String score = null;
		private String userId = null;
		private String movieId = null;
		
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
			int  rating;
			/* HTable row is constructed as
			 * userId::movieId    column=rating:score, timestamp=xxxxxxxxxxxxx, value=score
			 */
			String rowId = Bytes.toStringBinary(row.get()); 
			ratingInfo = rowId.split("::");
			userId = ratingInfo[0];
			movieId = ratingInfo[1];
			System.out.println("DEBUG: Mapper userId="+userId+", movieId="+movieId);
			//Get values from all columns
			for(KeyValue kv : columns.list()){
				score = Bytes.toStringBinary(kv.getValue());
				rating = Integer.parseInt(score);
				mr.setMovieId(new Text(movieId));
				mr.setRating(new IntWritable(rating));
//				mr = new MovieRatingWritable(new Text(movieId), new IntWritable(rating));
				System.out.println("DEBUG: Mapper movieId="+mr.getMovieId().toString()
						+", rating="+mr.getRating().get());
			}
			context.write(new Text(userId), mr);
		}
	}
	
	/**
	 * Calculate the average rating for a movie
	 * @author songyao
	 *
	 */
	static class RatingReducer1 extends Reducer<Text, MovieRatingWritable, MovieIdWritable, MovieRatingW>{
		
		MovieIdWritable mid = new MovieIdWritable();
		MovieRatingW mrating = new MovieRatingW();
		String movieA = null;
		String movieB = null;
		@Override
		protected void reduce(Text key, Iterable<MovieRatingWritable> values, 
				Context context) throws IOException ,InterruptedException {
			
			ArrayList<String> IdList = new ArrayList<String>();
			//ArrayList<IntWritable> RatingList = new ArrayList<IntWritable>();
			HashMap<String, Integer> MovieRatingMap = new HashMap<String, Integer>(); 
			for(MovieRatingWritable movie : values){
				IdList.add(movie.getMovieId().toString());
				MovieRatingMap.put(movie.getMovieId().toString(), movie.getRating().get());
//				mid.setMovieA(movie.getMovieId());
//				mid.setMovieB(movie.getMovieId());
//				mrating.setRatingA(movie.getRating());
//				mrating.setRatingB(movie.getRating());
				//context.write(mid, mrating);
			}

			for(int i=0;i<IdList.size();i++){
				movieA = IdList.get(i);
				mid.setMovieA(new Text(movieA));
				mrating.setRatingA(new IntWritable(MovieRatingMap.get(movieA)));
				for(int j=i+1;j<IdList.size();j++){
					movieB = IdList.get(j);
					mid.setMovieB(new Text(movieB));
					mrating.setRatingB(new IntWritable(MovieRatingMap.get(movieB)));
					context.write(mid, mrating);
				}
			}
			
		}
	}
	
	static class RatingMapper2 extends Mapper<MovieIdWritable, MovieRatingW,MovieIdWritable, MovieRatingW>{
				
		@SuppressWarnings("deprecation")
		@Override
		public void map(MovieIdWritable key,  MovieRatingW value, Context context)
					throws IOException, InterruptedException{
			//Keep track the number of rows that has been processed
			context.write(key,value);
		}
	}
	
	static class RatingReducer2 extends Reducer<MovieIdWritable, MovieRatingW,MovieIdWritable, Text>{
		private double cos_sim = 0;
		private double correlation =0;
		private double jaccard = 0;
		MovieRatingW mrating = new MovieRatingW();
		private double xysum =0;
		private double xsum =0;
		private double ysum =0;
		private double x2sum =0;
		private double y2sum =0;
		private double div1 =0;
		private double div2 =0;
		private double div =0;
		private int ra=0;
		private int rb=0;
		private int countn=0;
		@Override
		protected void reduce(MovieIdWritable key, Iterable<MovieRatingW> values, 
				Context context) throws IOException ,InterruptedException {
			xysum=0;xsum=0;ysum=0;x2sum=0;y2sum=0;
			countn=0;
			for(MovieRatingW ratingpair: values){
				countn++;
				ra = ratingpair.getRatingA().get();
				rb = ratingpair.getRatingB().get();
				xysum += ra*rb;
				x2sum += ra*ra;
				y2sum += rb*rb;
				xsum += ra;
				ysum += rb;
			}
			div1 = Math.sqrt(countn * x2sum - xsum * xsum); 
			div2 = Math.sqrt(countn * y2sum - ysum * ysum);
			div = div1*div2;
			if (countn ==1){
				cos_sim = -1;
			}
			else{
				cos_sim = xysum/(Math.sqrt(x2sum)*Math.sqrt(y2sum));
			}
			if (div==0)
				{correlation = 0;}
			else{
				correlation = (countn * xysum - xsum * ysum)/ div; 
			}
			if (key.getMovieA().toString().equals("0458339")
					|| key.getMovieA().toString().equals("0848228")
					|| key.getMovieA().toString().equals("0372784")
					|| key.getMovieA().toString().equals("1300854")) {
				context.write(
					key,
					new Text(String.valueOf(cos_sim) + ":"
							+ String.valueOf(correlation) + ":"
							+ String.valueOf(jaccard) + "-----" 
							+ String.valueOf(countn))
					);
			}
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
		job.setJarByClass(MeasureRatingSimilarity.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.out.println("DEBUG: Input Output job setup");
		//Set up the table mapper phase, and reducer phase
		TableMapReduceUtil.initTableMapperJob(table, scan, RatingMapper1.class, 
									Text.class, MovieRatingWritable.class, job);
		job.setReducerClass(RatingReducer1.class);
		
		System.out.println("DEBUG: Map Reducer Setup Completed");
		
		job.setOutputKeyClass(MovieIdWritable.class);
		job.setOutputValueClass(MovieRatingW.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
				
		job.waitForCompletion(true);
		
		System.out.println("DEBUG: first mapreduce finishes, read output file");
		/***************************************
		 * DEBUG USE ONLY
		 * */
		 
//		SequenceFile.Reader reader = new SequenceFile.Reader(job.getConfiguration(),
//										SequenceFile.Reader.file(new Path(DEFAULT_OUTPUT, "part-r-00000")));
//		
//		MovieIdWritable id = new MovieIdWritable();
//		//DoubleWritable rating = new DoubleWritable();
//		MovieRatingW ratingw = new MovieRatingW();
//		while(reader.next(id, ratingw)){
//			System.out.println(
//					"movie 1 id = " + id.getMovieA().toString()+
//					" movie 2 id = " + id.getMovieB().toString() + 
//					", rating 1= " + ratingw.getRatingA().toString() + 
//					" rating 2= " + ratingw.getRatingB().toString()
//					);
//		}
//
//		reader.close();
		
		/***********End DEBUG *******************/
		
		//start second map reduce job
		
		Configuration conf2 =  new Configuration();
		job = Job.getInstance(conf2, " similarity mapreduce 2");
		job.setJarByClass(MeasureRatingSimilarity.class);
		job.setMapperClass(RatingMapper2.class);
		job.setReducerClass(RatingReducer2.class);
		
		job.setMapOutputKeyClass(MovieIdWritable.class);
		job.setMapOutputValueClass(MovieRatingW.class);
		job.setOutputKeyClass(MovieIdWritable.class);
		job.setOutputValueClass(MovieRatingW.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(DEFAULT_OUTPUT));
		FileOutputFormat.setOutputPath(job, new Path(FINAL_OUTPUT));
		
		job.waitForCompletion(true);
		/***************************************
		 * DEBUG USE ONLY
		 * */
		 
		SequenceFile.Reader reader1 = new SequenceFile.Reader(job.getConfiguration(),
										SequenceFile.Reader.file(new Path(FINAL_OUTPUT, "part-r-00000")));
		
		MovieIdWritable id1 = new MovieIdWritable();
		//DoubleWritable rating = new DoubleWritable();
		Text rating = new Text();
		while(reader1.next(id1, rating)){
			System.out.println(
					"movie 1 id = " + id1.getMovieA().toString()+
					" movie 2 id = " + id1.getMovieB().toString() + 
					" rating  = " + rating.toString()
					);
		}

		reader1.close();
		
		/***********End DEBUG *******************/
//		/***************************************
//		 * DEBUG USE ONLY
//		 * */
//		 
//		SequenceFile.Reader reader = new SequenceFile.Reader(job.getConfiguration(),
//										SequenceFile.Reader.file(new Path(FINAL_OUTPUT, "part-r-00000")));
//		
//		MovieIdWritable id = new MovieIdWritable();
//		//DoubleWritable rating = new DoubleWritable();
//		MovieRatingW rating = new MovieRatingW();
//		while(reader.next(id, rating)){
//			System.out.println(
//					"movie 1 id = " + id.getMovieA().toString()+
//					" movie 2 id = " + id.getMovieB().toString() + 
//					", rating 1= " + rating.getRatingA().toString() + 
//					" rating 2= " + rating.getRatingB().toString()
//					);
//		}
//
//		reader.close();
//		
//		/***********End DEBUG *******************/
		
	}

}
