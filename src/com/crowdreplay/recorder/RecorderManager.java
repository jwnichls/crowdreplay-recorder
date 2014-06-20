package com.crowdreplay.recorder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class RecorderManager implements Runnable
{	
	//*******************************************************************
	// Member Variables

	protected static final String RUNNING_STATUS = "Running";
	protected static final String STARTING_STATUS = "Starting";
	protected static final String STOPPED_STATUS = "Stopped";
	protected static final String STOPPING_STATUS = "Stopping";
	
	
	//*******************************************************************
	// Member Variables
	
	protected String				_dbName;
	protected String				_dbUser;
	protected String				_dbPassword;
	
	protected String				_twitterConsumerKey;
	protected String				_twitterConsumerSecret;
	
	protected boolean				_running;
	protected HashMap<Integer, Recorder> _recorders;
	
	protected Connection		  	_dbConnection;
	protected PreparedStatement		_queryRecordersStmt;
	
	protected boolean				_dontInsertTweets = false;
	protected String				_recordFileName = null;
	

	//*******************************************************************
	// Constructor

	public RecorderManager(String dbName, String dbUser, String dbPassword,
						   String twitterConsumerKey, String twitterConsumerSecret)
	{
		_dbName = dbName;
		_dbUser = dbUser;
		_dbPassword = dbPassword;
		
		_twitterConsumerKey = twitterConsumerKey;
		_twitterConsumerSecret = twitterConsumerSecret;
		
		_recorders = new HashMap<Integer, Recorder>();
	}

	@Override
	public void run() 
	{
		_running = true;
		ResultSet rsRecorders = null;
		ArrayList<Recorder> currentRecorders = new ArrayList<Recorder>();
		
		try
		{
			_dbConnection = createDBConnection();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		while(_running && _dbConnection != null)
		{
			currentRecorders.clear();

			try
			{
				if (_dbConnection.isClosed())
					_dbConnection = createDBConnection();
				
				rsRecorders = _queryRecordersStmt.executeQuery();
				
				while(rsRecorders.next())
				{
					Integer id = new Integer(rsRecorders.getInt("id"));
					String category = rsRecorders.getString("category");
					String query = rsRecorders.getString("query");
					String accessToken = rsRecorders.getString("oauth_access_token");
					String accessSecret = rsRecorders.getString("oauth_access_secret");
					String status = rsRecorders.getString("status");
					
					Recorder r = _recorders.remove(id);
			
					if (r != null && 
						(!r.getQuery().equals(query) || 
						 !r.getCategory().equals(category) ||
						 !r.getOAuthAccessToken().equals(accessToken) ||
						 !r.getOAuthAccessSecret().equals(accessSecret)))
					{
						r.stop();
						r = null;
					}

					if (r == null)
					{
						r = createRecorder(id.intValue(), _twitterConsumerKey, _twitterConsumerSecret);
					}

					if (!r.isRunning() && (status.equals(STARTING_STATUS) || status.equals(RUNNING_STATUS)))
					{
						r.start();
					}
					else if (r.isRunning() && (status.equals(STOPPING_STATUS) || status.equals(STOPPED_STATUS)))
					{
						r.stop();
					}
					
					currentRecorders.add(r);
				}
				
				Iterator<Recorder> rIter = _recorders.values().iterator();
				while(rIter.hasNext())
					rIter.next().stop();
				_recorders.clear();
				
				rIter = currentRecorders.iterator();
				while(rIter.hasNext())
				{
					Recorder cr = rIter.next();
					_recorders.put(new Integer(cr.getId()), cr);
				}
				
				Thread.sleep(15000l);
			}
			catch(Exception e)
			{
				System.err.println("Exception in recorder manager thread");
				e.printStackTrace();
			}
			finally
			{
				try
				{
					if (rsRecorders != null)
						rsRecorders.close();					
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public void stop()
	{
		_running = false;
	}
	
	protected Recorder createRecorder(int id, String consumerKey, String consumerSecret) throws SQLException
	{
		Connection recorderDBConnection = createDBConnection();
		Recorder r = new Recorder(recorderDBConnection, id, consumerKey, consumerSecret);
		r.setDontInsertTweets(_dontInsertTweets);
		if (_recordFileName != null)
			r.setRecordToFile(_recordFileName);
		
		return r;
	}
	
	protected Connection createDBConnection() throws SQLException
	{
		Connection conn = DriverManager.getConnection(_dbName, _dbUser, _dbPassword);
		_queryRecordersStmt = conn.prepareStatement("select * from recorders;");
		
		return conn;
	}

	public void setDontInsertTweets(boolean value) 
	{
		_dontInsertTweets = value;
		
		Iterator<Recorder> recIterator = _recorders.values().iterator();
		while(recIterator.hasNext())
		{
			recIterator.next().setDontInsertTweets(_dontInsertTweets);
		}
	}
	
	public void setRecordToFileName(String filename)
	{
		_recordFileName = filename;
	}
}
