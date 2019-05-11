package com.jobreadyprogrammer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Dataset1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.setProperty("hadoop.home.dir", "D:\\winutils");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("dataset test").master("local[*]")
												   .config("spark.sql.warehouse.dir" , "file:///c:/tmp/")
												   .enableHiveSupport()
												   .getOrCreate();
		
		
		Dataset<Row>  dataset = spark.read().option("header" , true)
											 .csv("src/main/resources/students.csv");
		
		
		Long count = dataset.count();
		
		System.out.println("Total number of records available  " + String.valueOf(count));
		
		spark.sql("use ipl");
		/*spark.sql("create table students ( student_id int ,"
				+ "	exam_center_id int ,"
				+ "subject string ,"
				+ " year int , "
				+ "quarter int , "
				+ "score int , "
				+ "grade string )"
				+ "row format delimited fields terminated by ',' "
				+ "stored as textfile"
				);
		
		spark.sql("show tables").show();*/
		
		//spark.sql("load data local inpath 'src/main/resources/students.csv' overwrite into table students") ;
		spark.sql("select subject , count(student_id) from students group by subject ").show() ;
		
		
	/*	dataset.createOrReplaceTempView("students");
		
		Dataset<Row> results = spark.sql("select subject , max(score) , min(score) , avg(score) from students group by Subject" );
		
		results.coalesce(1);
		results.write().csv("amoldada1.csv"); ;
		*/
		
		
		
		
		
		
		
		

	}

}
