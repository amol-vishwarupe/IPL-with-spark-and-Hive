package com.jobreadyprogrammer.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class firstSpark {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		System.out.println("This is the first Spark program");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("firstspark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<Integer , Integer>> visitsRaw = new ArrayList<>();
		
		visitsRaw.add(new Tuple2<>(4,18));
		visitsRaw.add(new Tuple2<>(5,25));
		visitsRaw.add(new Tuple2<>(6,20));
		//visitsRaw.add(new Tuple2<>(7,18));
		
		
		
		List<Tuple2<Integer , String>> userssRaw = new ArrayList<>();
		
		userssRaw.add(new Tuple2<>(4,"Amol Vishwarupe"));
		userssRaw.add(new Tuple2<>(5,"sachin"));
		userssRaw.add(new Tuple2<>(6,"RAmesh"));
		userssRaw.add(new Tuple2<>(7,"Nana"));
		
		
		JavaPairRDD <Integer , Integer> visits = sc.parallelizePairs(visitsRaw);
		JavaPairRDD <Integer , String> users = sc.parallelizePairs(userssRaw);
		
		//Join pairRDDS by innerjoin		
		JavaPairRDD<Integer, Tuple2<Integer, String>> JoinedRDD = visits.join(users);
		System.out.println("Inner join ");
		JoinedRDD.collect().forEach(System.out::println);
		
		
		//Join pairRDDS by leftjoin		
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> LeftJoinedRDD = visits.rightOuterJoin(users);
		System.out.println("Leftouter join ");
		LeftJoinedRDD.collect().forEach(System.out::println);
		
		
		
		// Close the context
		
		sc.close();
		
		
		
	}

}
