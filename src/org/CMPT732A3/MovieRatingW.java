package org.CMPT732A3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class MovieRatingW implements WritableComparable<MovieRatingW>{

	private IntWritable ratingA;
	private IntWritable ratingB;
//	private IntWritable ratingAN= new IntWritable(0);
//	private IntWritable ratingBN= new IntWritable(0);
	
	public MovieRatingW(IntWritable ratingA, IntWritable ratingB, IntWritable ratingAN, IntWritable ratingBN) {
		this.ratingA = ratingA;
		this.ratingB = ratingB;
//		this.ratingAN = ratingAN;
//		this.ratingBN = ratingBN;
	}
	
	public MovieRatingW() {
		this.ratingA = new IntWritable();
		this.ratingB = new IntWritable();
//		this.ratingAN = new IntWritable(0);
//		this.ratingBN = new IntWritable(0);
	}
	
	
	/**
	 * @return the ratingA
	 */
	public IntWritable getRatingA() {
		return ratingA;
	}

	/**
	 * @param ratingA the ratingA to set
	 */
	public void setRatingA(IntWritable ratingA) {
		this.ratingA = ratingA;
	}

	/**
	 * @return the ratingB
	 */
	public IntWritable getRatingB() {
		return ratingB;
	}

	/**
	 * @param ratingB the ratingB to set
	 */
	public void setRatingB(IntWritable ratingB) {
		this.ratingB = ratingB;
	}

	/**
	 * @return the ratingAN
	 */
//	public IntWritable getRatingAN() {
//		return ratingAN;
//	}
//
//	/**
//	 * @param ratingAN the ratingAN to set
//	 */
//	public void setRatingAN(IntWritable ratingAN) {
//		this.ratingAN = ratingAN;
//	}
//
//	/**
//	 * @return the ratingBN
//	 */
//	public IntWritable getRatingBN() {
//		return ratingBN;
//	}
//
//	/**
//	 * @param ratingBN the ratingBN to set
//	 */
//	public void setRatingBN(IntWritable ratingBN) {
//		this.ratingBN = ratingBN;
//	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		ratingA.readFields(in);
		//ratingAN.readFields(in);
		ratingB.readFields(in);
		//ratingBN.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		ratingA.write(out);
		//ratingAN.write(out);
		ratingB.write(out);
		//ratingBN.write(out);
	}

	@Override
	public int compareTo(MovieRatingW mr) {
		// TODO Auto-generated method stub
		if( ratingA.compareTo(mr.getRatingA()) != 0){
			return ratingA.compareTo(mr.getRatingA());
		}
		
		
		return ratingB.compareTo(mr.getRatingB());
		
		
		//return ratingAN.compareTo(mr.getRatingAN());
		
	}


}
