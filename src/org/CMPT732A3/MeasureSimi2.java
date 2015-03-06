package org.CMPT732A3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MeasureSimi2 {
	final static String DEFAULT_OUTPUT = "SIMI_INITIAL";
	final static String INTER_OUTPUT = "SIMI_INTER";
	final static String FINAL_OUTPUT = "SIMI_FINAL_OUTPUT";
	final static String RATING_CONF_COLFAMILY = "conf.cf_rating";
	final static String RATING_CONF_QUALIFIER = "conf.qualifier_rating";
	final static String MOVIE_CONF_COLFAMILY = "conf.cf_movie";
	final static String MOVIE_CONF_QUALIFIER = "conf.qualifier_movie_rn";
	final static String SIMI_Table = "songyaos_simitable";
	
	public enum Counter {ROWS};
	
	/**
	 * Get rows of data from HTable as data source
	 * emits <movieID, rating> pairs
	 * @author songyao
	 *
	 */
	static class RatingMapper0 extends TableMapper<Text, Text>{
		
		private byte[] cf_rating;
		private byte[] qualifier_rating;
		private byte[] cf_movie;
		private byte[] qualifier_movie;
		private String[] ratingInfo;
		private String score = null;
		private String userId = null;
		private String movieId = null;
		private byte[]  rating;
		private String tableName;
		private byte[] movie_rating_number;
		private TableSplit currentSplit;
		@Override
		protected void setup(Context context) 
				throws IOException ,InterruptedException {
			// get column family and qualifer from context.getConfiguration()
			cf_rating = Bytes.toBytes(context.getConfiguration().get(RATING_CONF_COLFAMILY));
			qualifier_rating = Bytes.toBytes(context.getConfiguration().get(RATING_CONF_QUALIFIER));
			cf_movie = Bytes.toBytes(context.getConfiguration().get(MOVIE_CONF_COLFAMILY));
			qualifier_movie = Bytes.toBytes(context.getConfiguration().get(MOVIE_CONF_QUALIFIER));
		}
		
		@Override
		public void map(ImmutableBytesWritable row, Result columns, Context context)
					throws IOException, InterruptedException{
			//Keep track the number of rows that has been processed
			context.getCounter(Counter.ROWS).increment(1);
			
			/* HTable row is constructed as
			 * userId::movieId    column=rating:score, timestamp=xxxxxxxxxxxxx, value=score
			 */
			currentSplit = (TableSplit)context.getInputSplit();
			tableName = new String(currentSplit.getTableName()); //get current table name
			if(tableName.equals("songyaos_rating")){
				String rowKey = Bytes.toStringBinary(row.get()); 
				ratingInfo = rowKey.split("::");
				userId = ratingInfo[0];
				movieId = ratingInfo[1];
				rating = columns.getValue(cf_rating, qualifier_rating);
				context.write(new Text(movieId), new Text(userId + "-" + new String(rating)));
			}else if(tableName.equals("songyaos_movie")){
				movieId = Bytes.toStringBinary(row.get());
				movie_rating_number = columns.getValue(cf_movie, qualifier_movie);
				context.write(new Text(movieId), new Text("0" + "-" + new String(movie_rating_number,"UTF-8")));
			}
			
		}
	}
	
	static class RatingReducer0 extends Reducer<Text, Text, Text, MovieRatingWritable>{
		private int movie_rating_number = 0;
		private String userid;
		private String[] valuelist;
		private String rating;
		private MovieRatingWritable output = new MovieRatingWritable(); 
		@Override
		protected void reduce(Text key, Iterable<Text> values, 
				Context context) throws IOException ,InterruptedException {		
			List<String> mylist = new ArrayList<String>(); 
			for(Text value : values){
				mylist.add(value.toString());
				valuelist = value.toString().split("-");
				userid = valuelist[0];
				rating = valuelist[1];
				if (userid.equals("0")){
					movie_rating_number = Integer.parseInt(rating);
				}
			}
			
			for(String value : mylist){
				valuelist = value.split("-");
				userid = valuelist[0];
				rating = valuelist[1];
				if (!userid.equals("0")){
					output.setMovieId(key);
					output.setRating(new IntWritable(Integer.parseInt(rating)));
					output.setRating_number(new IntWritable(movie_rating_number));
					context.write(new Text(userid),output);
				}
			}
		}
	}
	
	static class RatingMapper1 extends Mapper<Text, MovieRatingWritable,Text, MovieRatingWritable>{
		@Override
		public void map(Text key, MovieRatingWritable value, Context context)
					throws IOException, InterruptedException{
			context.write(key, value);
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
			ArrayList<Integer> nList = new ArrayList<Integer>();
			HashMap<String, Integer> MovieRatingMap = new HashMap<String, Integer>(); 
			for(MovieRatingWritable movie : values){
				IdList.add(movie.getMovieId().toString());
				nList.add(movie.getRating_number().get());
				MovieRatingMap.put(movie.getMovieId().toString(), movie.getRating().get());
			}

			for(int i=0;i<IdList.size();i++){
				movieA = IdList.get(i);
				mid.setMovieA(new Text(movieA));
				mrating.setRatingA(new IntWritable(MovieRatingMap.get(movieA)));
				mid.setRatingAN(new IntWritable(nList.get(i)));
				for(int j=i+1;j<IdList.size();j++){
					movieB = IdList.get(j);
					mid.setMovieB(new Text(movieB));
					mrating.setRatingB(new IntWritable(MovieRatingMap.get(movieB)));
					mid.setRatingBN(new IntWritable(nList.get(j)));
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
		private int ran=0;
		private int rbn=0;
		private int countn=0;
		@Override
		protected void reduce(MovieIdWritable key, Iterable<MovieRatingW> values, 
				Context context) throws IOException ,InterruptedException {
			xysum=0;xsum=0;ysum=0;x2sum=0;y2sum=0;
			countn=0; div1=0;div2=0; ra=0;rb=0;ran=0;rbn=0; div=0;
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
			ran = key.getRatingAN().get();
			rbn = key.getRatingBN().get();
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
			jaccard = countn/((double)ran+rbn-countn);
			if (key.getMovieA().toString().equals("0458339")
					|| key.getMovieA().toString().equals("0848228")
					|| key.getMovieA().toString().equals("0372784")
					|| key.getMovieA().toString().equals("1300854") && countn > 1) {
				context.write(
					key,
					new Text(String.format("%.4f", correlation) + ":"
							+ String.format("%.4f",cos_sim) + ":"
							+ String.format("%.4f",jaccard) + ":" 
							+ String.valueOf(countn) + ":"
							+ String.valueOf(ran) + ":"
							+ String.valueOf(rbn)
							)
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
		String rating_table, movie_table, output, cf_rating, qualifier_rating, cf_movie, qualifier_movie_rn;
		if (args.length == 0){
			rating_table = ConstructRatingTable.RATING_TABLE;
			movie_table = ConstructMovieTable.MOVIE_TABLE;
			output = DEFAULT_OUTPUT;
			cf_rating = ConstructRatingTable.COLFAM_RATING_INFO;
			qualifier_rating = ConstructRatingTable.COL_RATING_SCORE;
			cf_movie = ConstructMovieTable.COLFAM_MOVIE_INFO;
			qualifier_movie_rn = ExtractRatingNumber.NEW_MOVIE_QUALIFIER;
			
		}else{
			rating_table = args[0];
			movie_table = args[1];
			output = args[2];
			cf_rating = args[3];
			qualifier_rating = args[4];
			cf_movie = args[5];
			qualifier_movie_rn = args[6];
		}
		
		//initialize 1st MapReduce 
		Configuration conf0 = HBaseConfiguration.create();
		conf0.set("hbase.zookeeper.quorum", 
				"rcg-hadoop-01.rcg.sfu.ca,rcg-hadoop-02.rcg.sfu.ca,rcg-hadoop-03.rcg.sfu.ca");
		conf0.set("zookeeper.znode.parent", "/hbase-unsecure");
		System.out.println("DEBUG: Configuration finished");
		// we can pass column family and qualifier in conf to reducers 
		conf0.set(RATING_CONF_COLFAMILY, cf_rating);
		conf0.set(RATING_CONF_QUALIFIER, qualifier_rating);
		conf0.set(MOVIE_CONF_COLFAMILY, cf_movie);
		conf0.set(MOVIE_CONF_QUALIFIER, qualifier_movie_rn);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf0, "parse table");
		
		//set up multiple table scan list
		List<Scan> scans = new ArrayList<Scan>();
		Scan movie_scan = new Scan();
		movie_scan.setAttribute("scan.attributes.table.name", Bytes.toBytes(movie_table));
		System.out.println(new String(movie_scan.getAttribute("scan.attributes.table.name")));
		Scan rating_scan = new Scan();
		rating_scan.setAttribute("scan.attributes.table.name", Bytes.toBytes(rating_table));
		System.out.println(Bytes.toStringBinary(rating_scan.getAttribute("scan.attributes.table.name")));
		rating_scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		movie_scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		rating_scan.setCacheBlocks(false);  // don't set to true for MR jobs
		movie_scan.setCacheBlocks(false);
		scans.add(movie_scan);
		scans.add(rating_scan);
		TableMapReduceUtil.initTableMapperJob(
				  scans,		            	 // Scan instance to control CF and attribute selection
				  RatingMapper0.class,   	 // mapper
				  Text.class,             	 // mapper output key
				  Text.class,             	 // mapper output value
				  job);

		//set up job
		job.setReducerClass(RatingReducer0.class);
		job.setJarByClass(MeasureRatingSimilarity.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MovieRatingWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job 0!");
		}
		System.out.println("DEBUG: first mapreduce finishes, read output file");
		/* DEBUG USE ONLY*/
		 
//		SequenceFile.Reader reader2 = new SequenceFile.Reader(job.getConfiguration(),
//										SequenceFile.Reader.file(new Path(DEFAULT_OUTPUT, "part-r-00000")));
//		
//		Text userid = new Text();
//		MovieRatingWritable value = new MovieRatingWritable();
//		while(reader2.next(userid, value)){
//			System.out.println(
//					"user id = " + userid.toString() +
//					", movie id = " + value.getMovieId().toString() + 
//					"  rating = " + value.getRating().toString() + 
//					"  rating# = " + value.getRating_number().toString()
//					);
//		}
//
//		reader2.close();
		
		/***********End DEBUG *******************/
		
		//initialize 2nd MapReduce 
		Configuration conf1 =  new Configuration();
		job = Job.getInstance(conf1, " similarity mapreduce 2");
		job.setJarByClass(MeasureRatingSimilarity.class);
		job.setMapperClass(RatingMapper1.class);
		job.setReducerClass(RatingReducer1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MovieRatingWritable.class);
		job.setOutputKeyClass(MovieIdWritable.class);
		job.setOutputValueClass(MovieRatingW.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(DEFAULT_OUTPUT));
		FileOutputFormat.setOutputPath(job, new Path(INTER_OUTPUT));
		
		b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job 1!");
		}
		
		
		//initialize 3rd MapReduce job
		Configuration conf2 =  new Configuration();
		job = Job.getInstance(conf2, " similarity mapreduce 3");
		job.setJarByClass(MeasureRatingSimilarity.class);
		job.setMapperClass(RatingMapper2.class);
		job.setReducerClass(RatingReducer2.class);
		job.setMapOutputKeyClass(MovieIdWritable.class);
		job.setMapOutputValueClass(MovieRatingW.class);
		job.setOutputKeyClass(MovieIdWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(INTER_OUTPUT));
		FileOutputFormat.setOutputPath(job, new Path(FINAL_OUTPUT));
		
		b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job 2!");
		}

		 /* DEBUG USE ONLY*/
		 
		SequenceFile.Reader reader = new SequenceFile.Reader(job.getConfiguration(),
										SequenceFile.Reader.file(new Path(FINAL_OUTPUT, "part-r-00000")));
		
		MovieIdWritable id = new MovieIdWritable();
		//DoubleWritable rating = new DoubleWritable();
		Text rating = new Text();
		String[] ratingtuple;
		while(reader.next(id, rating)){
			ratingtuple = rating.toString().split(":");
			System.out.println(
					"movie 1 id = " + id.getMovieA().toString()+
					" movie 2 id = " + id.getMovieB().toString() + 
					",  correlation = " + ratingtuple[0] + 
					"  cosine = " + ratingtuple[1] +
					" jaccard = " + ratingtuple[2] +
					" rating_count = " + ratingtuple[3]+
					" ran = " + id.getRatingAN().toString()+
					" rbn = " + id.getRatingBN().toString()
					);
		}

		reader.close();
		
		/***********End DEBUG *******************/
	}

}
