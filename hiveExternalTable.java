package com.jobreadyprogrammer.spark;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class hiveExternalTable {

	
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
		
		
		// Create table iplteams
		spark.sql("drop table player_external_partition");
		spark.sql("create external table player_external_partition ( player_name string )"							
							+ " stored as textfile "
							+ " row format delimited fields terminated by ',' "
							+ " partitioned by (country string )"
							+ " location 'src/main/resources/data/'"
					
				);
	
		spark.sql("ALTER TABLE player_external_partition ADD PARTITION (country ='BHARAT') LOCATION 'india/'");
		spark.sql("ALTER TABLE player_external_partition ADD PARTITION (country ='AUSTRALIA') LOCATION 'australia/'");
		spark.sql("ALTER TABLE player_external_partition ADD PARTITION (country ='PAKISTAN') LOCATION 'pakistan/'");
			
		spark.sql("select * from player_external_partition ").show();

	}

}
