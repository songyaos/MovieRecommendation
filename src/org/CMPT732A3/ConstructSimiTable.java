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

public class ConstructSimiTable {
	final static String SIMI_TABLE = "songyaos_simitable";
	final static String COLFAM_SIMI_INFO = "simi";
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
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(SIMI_TABLE));
		
		//Check if the table already exists
		HTableDescriptor[] tables = admin.listTables();
		for (int i=0; i<tables.length; i++){
			if(Bytes.equals(tables[i].getName(), htd.getName())){
				System.out.println("ERROR: Table "+htd.getNameAsString()+" already exists. Operation stops");
				return;
			}
		}
		
		//Create the Table with Column Family
		HColumnDescriptor hcd = new HColumnDescriptor(COLFAM_SIMI_INFO);
		htd.addFamily(hcd);				
		admin.createTable(htd);
		
		//HTable hTable = new HTable(conf, htd.getName());
				
		
		
	}
}
