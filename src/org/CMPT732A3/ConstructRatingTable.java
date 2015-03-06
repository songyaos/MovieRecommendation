package org.CMPT732A3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This program reads the DAT file, and creates the data to HTable
 * @author songyao
 *
 */
public class ConstructRatingTable {

	//final static String RATING_FILE = "sample_rating.dat";
	final static String RATING_FILE = "ratings.dat";
	//final static String RATING_TABLE = "songyaos_ratingInfo";
	final static String RATING_TABLE = "songyaos_rating";
	final static String COLFAM_RATING_INFO = "rating";
	final static String COL_RATING_SCORE = "score";
	/**
	 * @param args
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws IOException {
		
		//Seeds configuration object with required info to establish client connection
		Configuration conf = HBaseConfiguration.create();
		
		//Setup Table name
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(RATING_TABLE));
		
		//Check if the table already exists
		HTableDescriptor[] tables = admin.listTables();
		for (int i=0; i<tables.length; i++){
			if(Bytes.equals(tables[i].getName(), htd.getName())){
				System.out.println("ERROR: Table "+htd.getNameAsString()+" already exists. Operation stops");
				return;
			}
		}
		
		//Create the Table with Column Family
		HColumnDescriptor hcd = new HColumnDescriptor(COLFAM_RATING_INFO);
		htd.addFamily(hcd);				
		admin.createTable(htd);
		
		HTable hTable = new HTable(conf, htd.getName());
		
		//Enable client-side buffer
		if(hTable.isAutoFlush()){
			System.out.println("Enable Client-side Buffer for faster input");
			hTable.setAutoFlushTo(false);
		}
				
		try{
			String[] ratingInfo;
			String userId, movieId, rating;
			String prevUserId = null;
			
			//Read .dat file from HDFS, and parse it 
			BufferedReader br = new BufferedReader(new FileReader(new File(RATING_FILE)));
			String line = br.readLine();
			while(line != null){
				System.out.println("current_line: " + line);
				ratingInfo = line.split("::");
				userId = ratingInfo[0];
				movieId = ratingInfo[1];
				rating = ratingInfo[2];
				//force flush for every different user
				if(!userId.equals(prevUserId)){
					System.out.println("Flush for user " + prevUserId);
					prevUserId = userId;
					hTable.flushCommits();
				}
				// Store value to HTable
				Put put = new Put(Bytes.toBytes(userId+"::"+movieId));
				put.add(Bytes.toBytes(COLFAM_RATING_INFO), Bytes.toBytes(COL_RATING_SCORE), Bytes.toBytes(rating));
				hTable.put(put);
				
				line = br.readLine();
			}
			hTable.flushCommits();
			
			
		}catch(FileNotFoundException fex){
			System.out.println("Can't find existing file "+ RATING_FILE);
		}		
		
	}

}
