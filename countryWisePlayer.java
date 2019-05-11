package com.ipl.analysis;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class countryWisePlayer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/*
		 * 
		 * Build the report on the grounds on percentage of match winners were toss winners .
		 * 
		 * 
		 * 
		 */
		
		
		// TODO Auto-generated method stub
		
				System.setProperty("hadoop.home.dir", "D:\\winutils");
				Logger.getLogger("org.apache").setLevel(Level.OFF);
				
				SparkSession spark = SparkSession.builder().appName("dataset test").master("local[*]")
														   .config("spark.sql.warehouse.dir" , "file:///c:/tmp/")
														   .enableHiveSupport()
														   .getOrCreate();
				
				
					
				System.out.println(" Countrywise player participation  : ");
				spark.sql("use ipl");
				spark.sql("Select country , count(*) from player group by country order by count(*) desc");
				
		        Dataset<Row> tossmatch = spark.sql("select city_name as city_name_toss , count(*) as wintoss_match  from match where toss_winner_id = match_winner_id  group by city_name order by count(*) desc ");
		        
		        Dataset<Row> totalmatch = spark.sql("select city_name as city_name_tol , count(*) as match_count  from match  group by city_name order by count(*) desc ") ;
		        
		        Dataset<Row> results = totalmatch.join(tossmatch,  totalmatch.col("city_name_tol").equalTo(tossmatch.col("city_name_toss")), "left_outer");
				
		        Dataset<Row> results1 = results.withColumn("Percentage",  results.col("wintoss_match").multiply(100).divide(results.col("match_count")));
		        
		        results1.show();
		        
		               
		        results1.coalesce(1).write().csv("tosswinner_matchWinne1.csv");
				
		        

	}

}
