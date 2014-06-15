package com.crowdreplay.recorder;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class DBConverter {

	/**
	 * @param args
	 */
	public static void main(String[] args) 
	{
		int FETCH_SIZE = 1000;
		
		if (args.length < 2)
		{
			System.out.println("Usage: java TwitterRecorderDaemon <properties file name>");
			System.exit(-1);
		}
		
		try
		{
			Properties prop = new Properties();
			prop.load(new FileInputStream(args[0]));
			
			String dbName = "jdbc:mysql://127.0.0.1/" + prop.getProperty(TwitterRecorderDaemon.DB_NAME_PROP);
			String dbUser = prop.getProperty(TwitterRecorderDaemon.DB_USER_PROP);
			String dbPassword = prop.getProperty(TwitterRecorderDaemon.DB_PASSWORD_PROP);

			String oldDBName = "jdbc:mysql://127.0.0.1/" + args[1];

			Connection oldDBConnection = DriverManager.getConnection(oldDBName, dbUser, dbPassword);
			Connection newDBConnection = DriverManager.getConnection(dbName, dbUser, dbPassword);

			PreparedStatement getTweets = oldDBConnection.prepareStatement("select tweets.* from tweets inner join tweet_categories_tweets on tweets.id = tweet_categories_tweets.tweet_id where tweet_categories_tweets.tweet_category_id = ?;");

			// step 1. get the categories from the old database
			
			PreparedStatement categoryStmt = oldDBConnection.prepareStatement("select * from tweet_categories;");
			ResultSet categories = categoryStmt.executeQuery();
			
			// step 2. for each old category...
			while(categories.next())
			{
				int categoryId = categories.getInt("id");
				String tableName = "tweets_" + categoryId;
				
				System.out.println("Processing category " + categoryId + " : " + tableName);
				
				// step 2.1. create a table for the old category in the new database
				try
				{
					PreparedStatement createTableStmt = newDBConnection.prepareStatement("create table " + tableName + " like tweets;");
					createTableStmt.execute();
				}
				catch(Exception e2)
				{
					System.err.println("-W- Category table already exists: " + tableName);
				}
				
				// step 2.2. batch transfer tweets for the old category into the new database and table
				getTweets.setInt(1, categoryId);
				PreparedStatement insertTweets = newDBConnection.prepareStatement("insert into " + tableName + " (id, text, created_at, updated_at, screenname, user_id, lang, json, tweet_category_id) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? );");
				getTweets.setFetchSize(FETCH_SIZE);
				
				
				ResultSet tweets = getTweets.executeQuery();
				int counter = 0;
				while(tweets.next())
				{	
					insertTweets.setLong(1, tweets.getLong("id")); // id
					insertTweets.setString(2, tweets.getString("text")); // text
					insertTweets.setTimestamp(3, tweets.getTimestamp("created_at")); // created_at
					insertTweets.setTimestamp(4, tweets.getTimestamp("updated_at")); // updated_at
					insertTweets.setString(5, tweets.getString("screenname")); // screenname
					insertTweets.setInt(6, tweets.getInt("user_id")); // user_id
					insertTweets.setString(7, tweets.getString("lang")); // lang
					insertTweets.setString(8, tweets.getString("json")); // json
					insertTweets.setInt(9, categoryId);					
					insertTweets.addBatch();
					
					if (++counter > FETCH_SIZE)
					{
						System.out.println("Inserting 1000 tweets...");
						insertTweets.executeBatch();
						counter = 0;
					}
				}
				
				insertTweets.executeBatch();
				tweets.close();
			}			
			
			categories.close();
			
			oldDBConnection.close();
			newDBConnection.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

}
