package com.jobreadyprogrammer.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class createHiveTables {
	
	/*Description :
	 * below code will be generating all ipl related tables in ipl database
	 * match : match details
	 * perball : ball by ball score
	 * player : player details
	 * player_match : player per match details
	 * season : season details
	 * Team : team description
	 * 
	 * 
	 */
	
	
		public static void main(String[] args) 
		{
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
			spark.sql("drop table iplteams");
			spark.sql("create table iplteams ( team_id int ,"
					+ "	team_name string ,"
					+ " team_code string )"
					+ "row format delimited fields terminated by ',' "
					+ "stored as textfile "
					+ " TBLPROPERTIES (\"skip.header.line.count\" =\"1\")"					
					);
			
			//Load data into team table 
			spark.sql("load data local inpath 'src/main/resources/Team.csv' overwrite into table iplteams") ;
			spark.sql("select * from iplteams").show();
			
				
			// create table player
		spark.sql("drop table player");	
		spark.sql("create table player ( player_id int ,"
					+ "	player_name string ,"
					+ " dob string , "
					+ " batting_hand string ,"
					+ " bowling_skill string ,"
					+ " country string ,"
					+ " is_umpire int "
					+ " )"
					+ "row format delimited fields terminated by ',' "
					+ "stored as textfile "				
					);
			
			//Load data into team table 
			spark.sql("load data local inpath 'src/main/resources/Player.csv' overwrite into table player") ;
			spark.sql("select * from player").show();
	
			// Create table Match
	
			//Match_Id,Match_Date,Team_Name_Id,Opponent_Team_Id,Season_Id,Venue_Name,Toss_Winner_Id,Toss_Decision,IS_Superover,
			//IS_Result,Is_DuckWorthLewis,Win_Type,Won_By,Match_Winner_Id,Man_Of_The_Match_Id,First_Umpire_Id,Second_Umpire_Id,City_Name,Host_Country
			*/
			spark.sql("drop table match");	
			spark.sql("create table match ( match_id int ,"
						+ "	match_date string ,"
						+ " team_name_id int , "
						+ " opponent_team_id int ,"
						+ " season_id int ,"
						+ " venue_name string ,"
						+ " toss_winner_Id int ,"
						+ " toss_decision string ,"
						+ " is_superover int ,"
						+ " is_result int ,"
						+ " is_duckWorthlewis int ,"
						+ " win_type string ,"
						+ " won_by int ,"
						+ " match_winner_id int ,"
						+ " man_of_the_match_id int ,"
						+ " first_umpire_id int ,"
						+ " second_umpire_id int ,"
						+ " city_name string ,"
						+ " host_country string "						
						+ " )"
						+ "row format delimited fields terminated by ',' "
						+ "stored as textfile "				
						);
				
				//Load data into team table 
				spark.sql("load data local inpath 'src/main/resources/Match.csv' overwrite into table match") ;
				spark.sql("select * from match").show();
			
		/*	//Create table per ball analysis 
				
				// match_id,innings_id,over_id,ball_id,team_batting_id,team_bowling_id,striker_id,striker_batting_position,
				//non_striker_id,bowler_id,batsman_scored,extra_type,extra_runs,player_dissimal_id,dissimal_type,fielder_id	
			
				spark.sql("drop table scorecard");	
				spark.sql("create table scorecard ( match_id int ,"
							+ "	innings_id int ,"
							+ " over_id int , "
							+ " ball_id int ,"
							+ " team_batting_id int ,"
							+ " team_bowling_id int ,"
							+ " striker_id int ,"
							+ " striker_batting_position int ,"
							+ " non_striker_id int ,"
							+ " bowler_id int ,"
							+ " batsman_scored int ,"
							+ " extra_type string ,"
							+ " extra_runs int ,"
							+ " player_dissimal_id int ,"
							+ " dissimal_type int ,"
							+ " fielder_id int "							
							+ " )"
							+ "row format delimited fields terminated by ',' "
							+ "stored as textfile "				
							);
					
					//Load data into team table 
					spark.sql("load data local inpath 'src/main/resources/perball.csv' overwrite into table scorecard") ;
					spark.sql("select * from scorecard").show();
					
					// Create table player match 
					//match_id,player_id,team_id,is_keeper,is_captain
			
			
					spark.sql("drop table player_match");	
					spark.sql("create table player_match ( match_id int ,"
								+ "	player_id int ,"
								+ " team_id int , "
								+ " is_keeper int ,"
								+ " is_captain int "
								+ " )"
								+ "row format delimited fields terminated by ',' "
								+ "stored as textfile "				
								);
						
						//Load data into team table 
						spark.sql("load data local inpath 'src/main/resources/Player_Match.csv' overwrite into table player_match") ;
						spark.sql("select * from player_match").show();	
				
						// Create table season
						//season_id,season_year,orange_cap_id,purple_cap_id,man_of_the_series_id
				
	
						spark.sql("drop table season");	
						spark.sql("create table season ( season_id int ,"
									+ "	season_year int ,"
									+ " orange_cap_id int , "
									+ " purple_cap_id int ,"
									+ " man_of_the_series_id int "
									+ " )"
									+ "row format delimited fields terminated by ',' "
									+ "stored as textfile "				
									);
							
							//Load data into team table 
							spark.sql("load data local inpath 'src/main/resources/Season.csv' overwrite into table season") ;
							spark.sql("select * from season").show();	
						*/
						
		}

	}

	
	

