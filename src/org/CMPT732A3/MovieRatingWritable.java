package org.CMPT732A3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MovieRatingWritable implements WritableComparable<MovieRatingWritable> {

	private Text movieId;
	private IntWritable rating;
	private IntWritable rating_number = new IntWritable(0);
	
	
	
	public MovieRatingWritable(){
		this.movieId = new Text();
		this.rating = new IntWritable();
		this.rating_number = new IntWritable();
	}
	public MovieRatingWritable(Text movieId, IntWritable rating, IntWritable rating_number){
		this.movieId = movieId;
		this.rating = rating;
		this.rating_number = rating_number;
	}
	public MovieRatingWritable(Text movieId, IntWritable rating){
		this.movieId = movieId;
		this.rating = rating;
		this.rating_number = new IntWritable(0);
	}
	
	public Text getMovieId() {
		return movieId;
	}

	public void setMovieId(Text movieId) {
		this.movieId = movieId;
	}

	public IntWritable getRating() {
		return rating;
	}

	public void setRating(IntWritable rating) {
		this.rating = rating;
	}

	/**
	 * @return the rating_number
	 */
	public IntWritable getRating_number() {
		return rating_number;
	}
	/**
	 * @param rating_number the rating_number to set
	 */
	
	public void setRating_number(IntWritable rating_number) {
		this.rating_number = rating_number;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		movieId.readFields(in);
		rating.readFields(in);
		rating_number.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		movieId.write(out);
		rating.write(out);
		rating_number.write(out);
	}


	@Override
	//If object is greater than the Object o, return 1
	//smaller, return -1
	//equal, return 0
	public int compareTo(MovieRatingWritable mr) {
//		MovieRating mr = (MovieRating) o;
		return rating.compareTo(mr.getRating());
	}




}
