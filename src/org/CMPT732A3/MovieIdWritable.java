package org.CMPT732A3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MovieIdWritable implements WritableComparable<MovieIdWritable> {

	private Text movieA;
	private Text movieB;
	private IntWritable ratingAN= new IntWritable(0);
	private IntWritable ratingBN= new IntWritable(0);
	
	public MovieIdWritable() {
		this.movieA = new Text();
		this.movieB = new Text();
		this.ratingAN = new IntWritable(0);
		this.ratingBN = new IntWritable(0);
	}

	public MovieIdWritable(Text movieA, Text movieB, IntWritable ratingAN, IntWritable ratingBN) {
		this.movieA = movieA;
		this.movieB = movieB;
		this.ratingAN = ratingAN;
		this.ratingBN = ratingBN;
		
	}
	
	public Text getMovieA() {
		return movieA;
	}

	public void setMovieA(Text movieA) {
		this.movieA = movieA;
	}

	public Text getMovieB() {
		return movieB;
	}

	public void setMovieB(Text movieB) {
		this.movieB = movieB;
	}
	
	/**
	 * @return the ratingAN
	 */
	public IntWritable getRatingAN() {
		return ratingAN;
	}

	/**
	 * @param ratingAN the ratingAN to set
	 */
	public void setRatingAN(IntWritable ratingAN) {
		this.ratingAN = ratingAN;
	}

	/**
	 * @return the ratingBN
	 */
	public IntWritable getRatingBN() {
		return ratingBN;
	}

	/**
	 * @param ratingBN the ratingBN to set
	 */
	public void setRatingBN(IntWritable ratingBN) {
		this.ratingBN = ratingBN;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		movieA.readFields(in);
		ratingAN.readFields(in);
		movieB.readFields(in);
		ratingBN.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		movieA.write(out);
		ratingAN.write(out);
		movieB.write(out);
		ratingBN.write(out);
	}

	@Override
	public int compareTo(MovieIdWritable mr) {
		// TODO Auto-generated method stub
		
		if(movieA.compareTo(mr.getMovieA()) == 0){
			return (movieB.compareTo(mr.getMovieB()));
		}else{
			return movieA.compareTo(mr.getMovieA());
		}
	}

}
