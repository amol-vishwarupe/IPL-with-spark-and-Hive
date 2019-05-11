package com.jobreadyprogrammer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class hivePartitionTAble {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		System.setProperty("hadoop.home.dir", "D:\\winutils");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("dataset test").master("local[*]")
												   .config("spark.sql.warehouse.dir" , "file:///c:/tmp/")
												   .enableHiveSupport()
												   .getOrCreate();
		
		
		spark.sql("use ipl");
		System.out.println("Build database IPL in progress  " );
		//spark.sql("create database ipl");
		
	/*	
		// Create table iplteams
		//spark.sql("drop table player_partition");
		spark.sql("create table player_partition ( player_name string )"
							+ " partitioned by ( country string ) "
							+ " stored as textfile "
					
				);
		*/
		
		spark.sql("insert into player_partition partition ( country = \"INDIA\" ) values (\"saurabh \") , (\"Raina\") ") ;
		spark.sql("insert into player_partition partition ( country = \"AUSTRALIA\" ) values (\"Ponting \") , (\"Warner\") ") ;
		spark.sql("insert into player_partition partition ( country = \"ENGLAND\" ) values (\"Ben \") , (\"Jhony\") ") ;
		
		spark.sql("select * from player_partition ").show();

	}

}
