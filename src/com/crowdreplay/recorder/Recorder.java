package com.crowdreplay.recorder;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TimeZone;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

public class Recorder implements StatusListener {

	//*******************************************************************
	// Constants

	public static final int ID_BUFFER_SIZE = 10000;
	public static final int TIMESTAMP_BUFFER_SIZE = 3;

	
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
	protected PreparedStatement _volumeInsertStmt;
	protected PreparedStatement _volumeUpdateStmt;
	protected PreparedStatement	_updateRunningStatusStmt;
	protected PreparedStatement _createNewTableStmt;
	protected PreparedStatement _lockTableStmt;
	protected PreparedStatement _unlockTableStmt;
	
	protected int				_recorderId;
	
	protected String			_category;
	protected int				_categoryId;
	protected String			_tableName;
	
	protected String			_query;
	
	protected boolean			_ready;
	
	protected int				_currentSecond;
	protected int[]				_secondsCount;
	
	protected java.sql.Timestamp _lastTimestamp;
	protected HashMap<Long, Integer> _timestampCount;
	protected LinkedList<Long> _timestampQueue;
	
	protected TwitterStream 	_twitterStream;
	
	protected Calendar			_utcCal;
	
	protected boolean			_DONT_INSERT_TWEETS = false;

	protected String			_RECORD_TO_FILE = null;
	protected PrintWriter		_recordWriter;

	
	//*******************************************************************
	// Constructor

	public Recorder(Connection dbConn, int recorderId, String consumerKey, String consumerSecret)
	{
		_dbConnection = dbConn;
		_recorderId = recorderId;
		_consumerKey = consumerKey;
		_consumerSecret = consumerSecret;
			
		_timestampCount = new HashMap<Long, Integer>(TIMESTAMP_BUFFER_SIZE);
		_timestampQueue = new LinkedList<Long>();
		
		_utcCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
				
		// create some prepared statements
		try
		{
			_getRecorderInfoStmt = _dbConnection.prepareStatement("select category, query, oauth_access_token, oauth_access_secret from recorders where id=?;");
			_limitInsertStmt = _dbConnection.prepareStatement("insert into rate_limits (skipcount, created_at, updated_at, tweet_category_id) values ( ?, ?, ?, ? );");
			_volumeInsertStmt = _dbConnection.prepareStatement("insert into tweet_volumes (time, count, tweet_category_id) values ( ?, ?, ? );");
			_volumeUpdateStmt = _dbConnection.prepareStatement("update tweet_volumes set count=? where time=? and tweet_category_id=?;");
			_updateRunningStatusStmt = _dbConnection.prepareStatement("update recorders set status=?, running=? where id=?;");
			_unlockTableStmt = _dbConnection.prepareStatement("unlock tables;");

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

	public boolean insertingTweets()
	{
		return !_DONT_INSERT_TWEETS;
	}

	public void setDontInsertTweets(boolean value)
	{
		_DONT_INSERT_TWEETS = value;
	}

	public String getRecordToFile()
	{
		return _RECORD_TO_FILE;
	}
	
	public void setRecordToFile(String filename)
	{
		_RECORD_TO_FILE = filename;
		
		try
		{
			if (_recordWriter != null)
			{
				_recordWriter.flush();
				_recordWriter.close();
			}
		
			_recordWriter = new PrintWriter(new FileWriter(_RECORD_TO_FILE + _categoryId));			
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
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
		return new java.sql.Timestamp(date.getTime());
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
			_createNewTableStmt = _dbConnection.prepareStatement("create table " + _tableName + " like tweets;");
			_lockTableStmt = _dbConnection.prepareStatement("lock tables " + _tableName + " write;");
			
			if (_RECORD_TO_FILE != null)
			{
				_recordWriter = new PrintWriter(new FileWriter(_RECORD_TO_FILE + _categoryId));
			}
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
				categoryInsert.setTimestamp(2, getNowTimestamp(), _utcCal);
				categoryInsert.setTimestamp(3, getNowTimestamp(), _utcCal);
				categoryInsert.executeUpdate();
				
				// get the id of the newly inserted category
				ResultSet genKeys = categoryInsert.getGeneratedKeys();
				if (genKeys.next())
				{
					setCategoryId(genKeys.getInt(1));
				}
				genKeys.close();				
				categoryInsert.close();
			}	
			
			categorySet.close();
			categoryQuery.close();
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
			// long start = System.currentTimeMillis();
			// countTweet(tweet);
			
			// System.out.println(tweet.getId() + " : " + _category + " : " + tweet.getCreatedAt().toGMTString() + " : " + tweet.getUser().getScreenName());
			
			long tweetId = tweet.getId();

			// parse the created at time				
			java.sql.Timestamp ts = new java.sql.Timestamp(tweet.getCreatedAt().getTime());
			incrementVolumeForTime(ts, 1);
				
			if (!_DONT_INSERT_TWEETS)
			{
				// insert the tweet into the database
				_tweetInsertStmt.setLong(1, tweetId); // id
				_tweetInsertStmt.setString(2, tweet.getText()); // text
				_tweetInsertStmt.setTimestamp(3, ts, _utcCal); // created_at
				_tweetInsertStmt.setTimestamp(4, ts, _utcCal); // updated_at
				_tweetInsertStmt.setString(5, tweet.getUser().getScreenName()); // screenname
				_tweetInsertStmt.setInt(6, (int)tweet.getUser().getId()); // user_id
				_tweetInsertStmt.setString(7, "en");
				_tweetInsertStmt.setString(8, "");
				_tweetInsertStmt.setInt(9, _categoryId);
				_tweetInsertStmt.executeUpdate();
				//_tweetInsertStmt.addBatch();
			}
			
			if (_recordWriter != null)
			{
				_recordWriter.println(DataObjectFactory.getRawJSON(tweet));
			}
			
			// calculate the volumes for tweets in the last recorded minute
			if (_lastTimestamp != null)
			{
				if (_lastTimestamp.getTime() < ts.getTime())
				{
					if ((ts.getTime() / 60000) > (_lastTimestamp.getTime() / 60000))
					{
						// it's a new minute
						insertVolumeForTime(_lastTimestamp);
						
						// insert the tweets from the last minute in a batch
						//_tweetInsertStmt.executeBatch();
					}
					_lastTimestamp = ts;
				}
			}
			else
			{
				_lastTimestamp = ts;
			}
		}
		catch(com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException ce)
		{
			if (ce.getMessage().indexOf("Duplicate entry") < 0)
			{
				// not a duplicate entry exception
				System.err.println("Error inserting a tweet");
				ce.printStackTrace();
			}
		}
		catch(Exception e)
		{
			System.err.println("Error inserting a tweet");
			e.printStackTrace();
		}		

	}

	protected long truncateTimestampAsLong(long time)
	{
		return (time - (time % 60000));
	}
	
	protected java.sql.Timestamp truncateTimestamp(java.sql.Timestamp ts)
	{
		return new java.sql.Timestamp(truncateTimestampAsLong(ts.getTime()));		
	}
	
	protected void incrementVolumeForTime(java.sql.Timestamp ts, int count)
	{
		long startTime = truncateTimestampAsLong(ts.getTime());
		if (_timestampCount.containsKey(startTime))
			count += _timestampCount.get(startTime);
		
		_timestampCount.put(startTime, count);
	}
	
	protected void insertVolumeForTime(java.sql.Timestamp ts)
	{
		try
		{
			// calculate start time for duration
			//   startTime is the beginning of the minute specified by ts
			long startTime = truncateTimestampAsLong(ts.getTime());
			
			// unlock tables
			// _unlockTableStmt.execute();

			// check to see if we're at capacity
			while (_timestampQueue.size() >= TIMESTAMP_BUFFER_SIZE && _timestampQueue.size() > 0)
			{
				// pop oldest timestamp from queue and remove from hashtable
				long finalTime = _timestampQueue.poll();
				java.sql.Timestamp finalTimestamp = new java.sql.Timestamp(finalTime);
				int count = _timestampCount.remove(finalTime).intValue();
				
				_volumeUpdateStmt.setInt(1, count);
				_volumeUpdateStmt.setTimestamp(2, finalTimestamp, _utcCal);
				_volumeUpdateStmt.setInt(3, _categoryId);
				_volumeUpdateStmt.executeUpdate();
					
				System.out.println("Volume Updated: " + finalTimestamp.toGMTString() + " " + count);
			}

			_timestampQueue.offer(startTime);
			int count = _timestampCount.get(startTime);
			java.sql.Timestamp startTimestamp = new java.sql.Timestamp(startTime);

			// save the volume count into the database
			_volumeInsertStmt.setTimestamp(1, startTimestamp, _utcCal);
			_volumeInsertStmt.setInt(2, count);
			_volumeInsertStmt.setInt(3, _categoryId);
			_volumeInsertStmt.executeUpdate();
			
			System.out.println("Volume Calculated and Stored: " + startTimestamp.toGMTString() + " " + count);	
			
			// relock the tweets table
			// _lockTableStmt.execute();
		}
		catch(Exception e)
		{
			System.err.println("Error calculating and insert volume information");
			e.printStackTrace();
		}
	}

	/*
	protected int getTweetCount(int categoryId, java.sql.Timestamp startTime) throws SQLException
	{
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

		return count;
	}
	*/
	
	@Override
	public void onTrackLimitationNotice(int statusesLost) 
	{
		try
		{
			// unlock tables
			// _unlockTableStmt.execute();
			
			Timestamp now = getNowUTCTimestamp();
			_limitInsertStmt.setInt(1, statusesLost);
			_limitInsertStmt.setTimestamp(2, now, _utcCal);
			_limitInsertStmt.setTimestamp(3, now, _utcCal);
			_limitInsertStmt.setInt(4, _categoryId);
			_limitInsertStmt.executeUpdate();
			
			incrementVolumeForTime(now, statusesLost);
			
			System.out.println("Rate limited received and stored: " + statusesLost);
			
			// relock tweets table
			// _lockTableStmt.execute();
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
				cb.setDebugEnabled(false)
				  .setJSONStoreEnabled(true)
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
			
			// lock the tweets table as we start recording
			// _lockTableStmt.execute();
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
				
				if (_lastTimestamp != null)
					insertVolumeForTime(_lastTimestamp);
				
				System.out.println("Recorder stopped " + _recorderId);
			}
			
			// unlock tables
			// _unlockTableStmt.execute();
			
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
			_volumeInsertStmt.close();
			_volumeUpdateStmt.close();
			_updateRunningStatusStmt.close();
			_createNewTableStmt.close();
			
			_dbConnection.close();
			
			if (_recordWriter != null)
			{
				_recordWriter.flush();
				_recordWriter.close();
			}
		}
		catch(Exception e)
		{
			System.err.println("Error trying to clean up recorder " + _recorderId);
			e.printStackTrace();
		}
	}
}
