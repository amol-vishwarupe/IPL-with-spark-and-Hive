package com.jobreadyprogrammer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class checkHiveDatabase {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.setProperty("hadoop.home.dir", "D:\\winutils");
		Logger.getLogger("org.apache").setLevel(Level.OFF);
		
		SparkSession spark = SparkSession.builder().appName("dataset test").master("local[*]")
												   .config("spark.sql.warehouse.dir" , "file:///c:/tmp/")
												   .enableHiveSupport()
												   .getOrCreate();
		
		
			
		System.out.println(" All databases available : ");
		spark.sql("Show Databases").show();
		
		spark.sql("use ipl");
		spark.sql("show tables").show();
		

	}



}


