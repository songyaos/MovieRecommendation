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
public class ConstructMovieTable {

	//final static String MOVIE_FILE = "sample_movie.dat";
	final static String MOVIE_FILE = "movies.dat";
	//final static String MOVIE_TABLE = "songyaos_movieInfo";
	final static String MOVIE_TABLE = "songyaos_movie";
	
	final static String COLFAM_MOVIE_INFO = "info";
	final static String COL_MOVIE_NAME = "name";
	final static String COL_MOVIE_GENRES  = "genres";
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
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(MOVIE_TABLE));
		
		//Check if the table already exists
		HTableDescriptor[] tables = admin.listTables();
		for (int i=0; i<tables.length; i++){
			if(Bytes.equals(tables[i].getName(), htd.getName())){
				System.out.println("ERROR: Table "+htd.getNameAsString()+" already exists. Operation stops");
				return;
			}
		}
		
		//Create the Table with Column Family
		HColumnDescriptor hcd = new HColumnDescriptor(COLFAM_MOVIE_INFO);
		htd.addFamily(hcd);				
		admin.createTable(htd);
		
		HTable hTable = new HTable(conf, htd.getName());
				
		try{
			String[] movieInfo;
			String movieId, movieName, genres;
			
			//Read .dat file from HDFS, and parse it 
			BufferedReader br = new BufferedReader(new FileReader(new File(MOVIE_FILE)));
			String line = br.readLine();
			while(line != null){
				System.out.println("current_line: " + line);
				movieInfo = line.split("::");
//				movieId = Integer.parseInt(movieInfo[0]);
				movieId = movieInfo[0];
				movieName = movieInfo[1];
				if (movieInfo.length ==3){
					genres = movieInfo[2];}
				else{genres = "N/A";}
				// Store value to HTable
				Put put = new Put(Bytes.toBytes(movieId));
				
				put.add(Bytes.toBytes(COLFAM_MOVIE_INFO), Bytes.toBytes(COL_MOVIE_NAME), Bytes.toBytes(movieName));
				put.add(Bytes.toBytes(COLFAM_MOVIE_INFO), Bytes.toBytes(COL_MOVIE_GENRES), Bytes.toBytes(genres));
				hTable.put(put);
				
				line = br.readLine();
			}
			
			
		}catch(FileNotFoundException fex){
			System.out.println("Can't find existing file "+ MOVIE_FILE);
		}		
		
	}

}
