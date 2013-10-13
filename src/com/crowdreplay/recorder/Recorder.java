package com.crowdreplay.recorder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class Recorder implements StatusListener {

	//*******************************************************************
	// Constants

	public static final int ID_BUFFER_SIZE = 10000;

	
	//*******************************************************************
	// Member Variables

	protected Connection 		_dbConnection;

	protected String 			_consumerKey;
	protected String 			_consumerSecret;
	protected String 			_accessToken;
	protected String 			_accessSecret;

	protected PreparedStatement _getRecorderInfoStmt;
	protected PreparedStatement _tweetInsertStmt;
	protected PreparedStatement _limitInsertStmt;
	protected PreparedStatement _tweetCountAtTimeStmt;
	protected PreparedStatement _rateLimitCountAtTimeStmt;
	protected PreparedStatement _volumeInsertStmt;
	protected PreparedStatement	_updateRunningStatusStmt;
	protected PreparedStatement _createNewTableStmt;
	
	protected int				_recorderId;
	
	protected String			_category;
	protected int				_categoryId;
	protected String			_tableName;
	
	protected String			_query;
	
	protected boolean			_ready;
	
	protected int				_currentSecond;
	protected int[]				_secondsCount;
	
	protected java.sql.Timestamp _lastTimestamp;
	
	protected TwitterStream 	_twitterStream;

	
	//*******************************************************************
	// Constructor

	public Recorder(Connection dbConn, int recorderId, String consumerKey, String consumerSecret)
	{
		_dbConnection = dbConn;
		_recorderId = recorderId;
		_consumerKey = consumerKey;
		_consumerSecret = consumerSecret;
				
		// create some prepared statements
		try
		{
			_getRecorderInfoStmt = _dbConnection.prepareStatement("select category, query, oauth_access_token, oauth_access_secret from recorders where id=?;");
			_limitInsertStmt = _dbConnection.prepareStatement("insert into rate_limits (skipcount, created_at, tweet_category_id) values ( ?, ?, ? );");
			_rateLimitCountAtTimeStmt = _dbConnection.prepareStatement("select sum(rate_limits.skipcount) as sum_id from rate_limits where (created_at >= ? AND created_at < ?) AND tweet_category_id = ?;");
			_volumeInsertStmt = _dbConnection.prepareStatement("insert into tweet_volumes (time, count, tweet_category_id) values ( ?, ?, ? );");
			_updateRunningStatusStmt = _dbConnection.prepareStatement("update recorders set status=?, running=? where id=?;");
			
			_getRecorderInfoStmt.setInt(1, recorderId);
			ResultSet rsRecorderInfo = _getRecorderInfoStmt.executeQuery();
			if (rsRecorderInfo.next())
			{				
				_category = rsRecorderInfo.getString(1);
				this.createOrFindCategory(_category);
				
				_query = rsRecorderInfo.getString(2);
				
				_accessToken = rsRecorderInfo.getString(3);
				_accessSecret = rsRecorderInfo.getString(4);
			}
			rsRecorderInfo.close();
			
			_ready = _category != null && _query != null && _accessToken != null && _accessSecret != null;
			
			_secondsCount = new int[60];
			_currentSecond = (new Date()).getSeconds();
			
			_lastTimestamp = null; 

			//System.out.println("Consumer Key: " + _consumerKey + "  Consumer Secret: " + _consumerSecret);
			//System.out.println("Access Token: " + _accessToken + "  Access Secret: " + _accessSecret);
		}
		catch(SQLException e)
		{
			System.err.println("Problem initializing recorder " + _recorderId);
			e.printStackTrace();
		}
	}
	
	//*******************************************************************
	// Member Methods
	
	public boolean isRunning() 
	{
		return _twitterStream != null;
	}
	
	public int getId()
	{
		return _recorderId;
	}
	
	public String getQuery()
	{
		return _query;
	}
	
	public String getCategory()
	{
		return _category;
	}
	
	public String getOAuthAccessToken()
	{
		return _accessToken;
	}
	
	public String getOAuthAccessSecret()
	{
		return _accessSecret;
	}
	
	public java.sql.Timestamp getNowUTCTimestamp()
	{
		java.util.Date date = new java.util.Date(); // now
		return new java.sql.Timestamp(date.getTime() + date.getTimezoneOffset()*60*1000);
	}

	public java.sql.Timestamp getNowTimestamp()
	{
		return new java.sql.Timestamp((new java.util.Date()).getTime());
	}
	
	public void setCategoryId(int id)
	{
		_categoryId = id;
		_tableName = "tweets_" + _categoryId;
		
		try
		{
			_tweetInsertStmt = _dbConnection.prepareStatement("insert into " + _tableName + " (id, text, created_at, updated_at, screenname, user_id, lang, json, tweet_category_id) values ( ?, ?, ?, ?, ?, ?, ?, ?, ? );");
			_tweetCountAtTimeStmt = _dbConnection.prepareStatement("select count(*) from " + _tableName + " WHERE (created_at >= ? AND created_at < ? AND tweet_category_id = ?)");
			_createNewTableStmt = _dbConnection.prepareStatement("create table " + _tableName + " like tweets;");
		}
		catch(Exception e)
		{
			System.err.println("Problem creating category statements for recorder " + _recorderId);
			e.printStackTrace();
		}
	}
	
	public String getTableName()
	{
		return _tableName;
	}

	public void createOrFindCategory(String category)
	{		
		try
		{
			PreparedStatement categoryQuery = _dbConnection.prepareStatement("select * from tweet_categories where category=?;");
			categoryQuery.setString(1, category);
			
			ResultSet categorySet = categoryQuery.executeQuery();
			if (categorySet.first())
			{
				// if we can go to a valid first row, then a category was found
				setCategoryId(categorySet.getInt("id"));
			}
			else
			{
				// insert the new category into the database
				PreparedStatement categoryInsert = _dbConnection.prepareStatement("insert into tweet_categories (category, created_at, updated_at) values ( ?, ?, ? );", Statement.RETURN_GENERATED_KEYS);
				categoryInsert.setString(1, category);
				categoryInsert.setTimestamp(2, getNowTimestamp());
				categoryInsert.setTimestamp(3, getNowTimestamp());
				categoryInsert.executeUpdate();
				
				// get the id of the newly inserted category
				ResultSet genKeys = categoryInsert.getGeneratedKeys();
				if (genKeys.next())
				{
					setCategoryId(genKeys.getInt(1));
				}
				genKeys.close();				
			}	
			
			categorySet.close();
		}
		catch(SQLException e)
		{
			System.err.println("Error finding/creating category: " + category);
			e.printStackTrace();
		}
		
		try
		{
			_createNewTableStmt.execute();
		}
		catch(SQLException e)
		{
			System.err.println("-WARNING- Problem creating new table for tweets: " + getTableName() + " (probably ok, since may have been created in the past)");
		}
	}

	@Override
	public void onException(Exception arg0) 
	{
		System.err.println("Recorder " + _recorderId + " threw exception.");
		arg0.printStackTrace();
	}

	@Override
	public void onDeletionNotice(StatusDeletionNotice arg0) 
	{
		// TODO: Delete tweet from database
	}

	@Override
	public void onScrubGeo(long arg0, long arg1) 
	{
		// Can ignore this for now, because we aren't saving geo information
	}

	@Override
	public void onStatus(Status tweet) {

		try
		{
			// countTweet(tweet);
			
			System.out.println(tweet.getId() + " : " + _category + " : " + tweet.getCreatedAt().toGMTString() + " : " + tweet.getUser().getScreenName());
			
			long tweetId = tweet.getId();

			// parse the created at time				
			Timestamp ts = new Timestamp(tweet.getCreatedAt().getTime() + tweet.getCreatedAt().getTimezoneOffset()*60*1000);

			// calculate the volumes for tweets in the last recorded minute
			if (_lastTimestamp != null &&
				((ts.getMinutes() != _lastTimestamp.getMinutes()) ||
				 (ts.getTime() - _lastTimestamp.getTime()) > 60000))
			{
				calculateVolumeForTime(_lastTimestamp);
			}
			_lastTimestamp = ts;
			
			_tweetInsertStmt.setLong(1, tweetId); // id
			_tweetInsertStmt.setString(2, tweet.getText()); // text
			_tweetInsertStmt.setTimestamp(3, ts); // created_at
			_tweetInsertStmt.setTimestamp(4, ts); // updated_at
			_tweetInsertStmt.setString(5, tweet.getUser().getScreenName()); // screenname
			_tweetInsertStmt.setInt(6, (int)tweet.getUser().getId()); // user_id
			_tweetInsertStmt.setString(7, "en");
			_tweetInsertStmt.setString(8, "");
			_tweetInsertStmt.setInt(9, _categoryId);
			_tweetInsertStmt.executeUpdate();				
		}
		catch(Exception e)
		{
			System.err.println("Error inserting a tweet");
			e.printStackTrace();
		}		

	}

	protected void countTweet(Status tweet)
	{
		int currentSec = tweet.getCreatedAt().getSeconds();
		int secDiff = currentSec - _currentSecond;
		if (secDiff == 0)
			_secondsCount[currentSec]++;
		else
		{
			_secondsCount[currentSec] = 1;
			for(int i = _currentSecond; i < currentSec; i++)
				_secondsCount[i] = 0;
		}
		_currentSecond = currentSec;
	}
	
	protected double calculateRate(Date now)
	{
		int currentSec = now.getSeconds() + 60;
		double total = 0;
		for(int i = 0; i < 15; i++)
		{
			total += _secondsCount[(currentSec - i) % 60];
		}
		
		return (total / 15);
	}
	
	protected void calculateVolumeForTime(java.sql.Timestamp ts)
	{
		try
		{
			// calculate start time and end time for duration
			//   startTime is the beginning of the minute specified by ts
			//   endTime is the beginning of the minute following ts
			java.sql.Timestamp startTime = (java.sql.Timestamp)ts.clone();
			startTime.setSeconds(0);
			startTime.setNanos(0);
			java.sql.Timestamp endTime = new java.sql.Timestamp(startTime.getTime() + 60000);
			
			// generate the tweet count for this unit of time
			_tweetCountAtTimeStmt.setTimestamp(1, startTime);
			_tweetCountAtTimeStmt.setTimestamp(2, endTime);
			_tweetCountAtTimeStmt.setInt(3, _categoryId);
			
			int count = 0;
			ResultSet rsCount = _tweetCountAtTimeStmt.executeQuery();
			if (rsCount.next())
				count += rsCount.getInt(1);
			rsCount.close();
			
			_rateLimitCountAtTimeStmt.setTimestamp(1, startTime);
			_rateLimitCountAtTimeStmt.setTimestamp(2, endTime);
			_rateLimitCountAtTimeStmt.setInt(3, _categoryId);
			
			rsCount = _rateLimitCountAtTimeStmt.executeQuery();
			if (rsCount.next())
				count += rsCount.getInt(1);
			rsCount.close();

			// save that volume count back into the database
			_volumeInsertStmt.setTimestamp(1, startTime);
			_volumeInsertStmt.setInt(2, count);
			_volumeInsertStmt.setInt(3, _categoryId);
			_volumeInsertStmt.executeUpdate();
			
			System.out.println("Volume Calculated and Stored: " + startTime.toGMTString() + " " + count);
		}
		catch(Exception e)
		{
			System.err.println("Error calculating and insert volume information");
			e.printStackTrace();
		}
	}
	
	@Override
	public void onTrackLimitationNotice(int statusesLost) 
	{
		try
		{
			_limitInsertStmt.setInt(1, statusesLost);
			_limitInsertStmt.setTimestamp(2, getNowUTCTimestamp());
			_limitInsertStmt.setInt(3, _categoryId);
			_limitInsertStmt.executeUpdate();
		}
		catch(SQLException e)
		{
			System.err.println("Error inserting a rate limit");
			e.printStackTrace();
		}				
	}
	
	@Override
	public void onStallWarning(StallWarning arg0) {
		// TODO Auto-generated method stub
		
	}

	protected String[] parseQueryStringForTrackTerms(String query)
	{
		ArrayList<String> terms = new ArrayList<String>(Arrays.asList(query.split("\\s+")));
		
		for(int i = 0; i < terms.size(); i++)
		{
			if (terms.get(i).startsWith("loc:"))
				terms.remove(i--);
		}
		
		return terms.toArray(new String[terms.size()]);
	}

	protected double[][] parseQueryStringForLocTerms(String query)
	{
		String[] terms = query.split("\\s+");
		ArrayList<double[]> locs = new ArrayList<double[]>();
		
		for(int i = 0; i < terms.length; i++)
		{
			if (terms[i].startsWith("loc:"))
			{
				String[] latlng = terms[i].substring(4).split(",");
				
				if (latlng.length == 4)
				{
					double[] loc = new double[2];
					for(int j = 0; j < 4; j++)
					{
						loc[j % 2] = Double.parseDouble(latlng[j]);
						
						if ((j % 2) == 1)
						{
							locs.add(loc);
							loc = new double[2];
						}
					}
				}
				else
				{
					System.err.println("Bad locations found: " + terms[i]);
				}
			}
		}
		
		return locs.toArray(new double[locs.size()][]);
	}

	public void start()
	{
		try
		{
			if (!isRunning())
			{
				System.out.println("Starting recorder with id " + _recorderId);
				System.out.println("Category: " + _category + "  Query: " + _query);
				
				// start twitter recording
				ConfigurationBuilder cb = new ConfigurationBuilder();
				cb.setDebugEnabled(true)
				  .setOAuthConsumerKey(_consumerKey)
				  .setOAuthConsumerSecret(_consumerSecret)
				  .setOAuthAccessToken(_accessToken)
				  .setOAuthAccessTokenSecret(_accessSecret);
				
				FilterQuery query = new FilterQuery(0, null, parseQueryStringForTrackTerms(_query), parseQueryStringForLocTerms(_query));
				
			    _twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
			    _twitterStream.addListener(this);
			    _twitterStream.filter(query);
			}
			
		    // update running status in the database
			_updateRunningStatusStmt.setString(1, RecorderManager.RUNNING_STATUS);
			_updateRunningStatusStmt.setBoolean(2, true);
			_updateRunningStatusStmt.setInt(3, _recorderId);
			_updateRunningStatusStmt.executeUpdate();
		}
		catch(Exception e)
		{
			System.err.println("Problem starting recorder");
			e.printStackTrace();
		}		
	}
	
	public void stop()
	{
		try
		{
			if (isRunning())
			{
				_twitterStream.shutdown();
				_twitterStream.cleanUp();
				_twitterStream = null;
				
				calculateVolumeForTime(_lastTimestamp);
				
				System.out.println("Recorder stopped " + _recorderId);
			}
			
		    // update running status in the database
			_updateRunningStatusStmt.setString(1, RecorderManager.STOPPED_STATUS);
			_updateRunningStatusStmt.setBoolean(2, false);
			_updateRunningStatusStmt.setInt(3, _recorderId);
			_updateRunningStatusStmt.executeUpdate();						
		}
		catch(Exception e)
		{
			System.err.println("Problem stopping recorder " + _recorderId);
			e.printStackTrace();
		}
	}
	
	public void destroy()
	{
		try
		{
			_getRecorderInfoStmt.close();
			_tweetInsertStmt.close();
			_limitInsertStmt.close();
			_tweetCountAtTimeStmt.close();
			_rateLimitCountAtTimeStmt.close();
			_volumeInsertStmt.close();
			_updateRunningStatusStmt.close();
			_createNewTableStmt.close();
		}
		catch(Exception e)
		{
			System.err.println("Error trying to clean up recorder " + _recorderId);
			e.printStackTrace();
		}
	}
}
