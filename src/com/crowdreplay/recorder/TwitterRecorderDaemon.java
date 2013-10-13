package com.crowdreplay.recorder;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

public class TwitterRecorderDaemon implements Daemon {

	//*******************************************************************
	// Constants

	public static final String DB_NAME_PROP 		= "dbName";
	public static final String DB_USER_PROP 		= "dbUser";
	public static final String DB_PASSWORD_PROP 	= "dbPassword";
	public static final String CONSUMER_KEY_PROP 	= "consumerKey";
	public static final String CONSUMER_SECRET_PROP = "consumerSecret";
	
	
	//*******************************************************************
	// Member Variables

	protected RecorderManager _twitterRecorderManager;
	protected Thread _managerThread;

	protected String _dbName;
	protected String _dbUser;
	protected String _dbPassword;
	protected String _consumerKey; 
	protected String _consumerSecret;

	
	//*******************************************************************
	// Constructor
	
	public TwitterRecorderDaemon(String dbName, String dbUser, String dbPassword, String consumerKey, String consumerSecret)
	{
		_dbName = "jdbc:mysql://127.0.0.1/" + dbName;
		_dbUser = dbUser;
		_dbPassword = dbPassword;
		_consumerKey = consumerKey;
		_consumerSecret = consumerSecret;
	}

	
	//*******************************************************************
	// Apache Daemon methods
	
	@Override
	public void init(DaemonContext arg0) throws DaemonInitException, Exception
	{
		// Here open configuration files, create a trace file, create ServerSockets, Threads
		_twitterRecorderManager = new RecorderManager(_dbName, _dbUser, _dbPassword, _consumerKey, _consumerSecret);
		_managerThread = new Thread(_twitterRecorderManager);
	}
	
	@Override
	public void start()
	{
		// Start the Thread, accept incoming connections
		_managerThread.start();
	}
	
	@Override
	public void stop()
	{
		// Inform the Thread to terminate the run(), close the ServerSockets
		_twitterRecorderManager.stop();
	}
	
	@Override
	public void destroy()
	{
		// Destroy any object created in init()
		_managerThread.destroy();
	}
	
	//*******************************************************************
	// Static Main Method

	public static void main(String[] args) 
	{
		if (args.length < 1)
		{
			System.out.println("Usage: java TwitterRecorderDaemon <properties file name>");
			System.exit(-1);
		}
		
		try
		{
			Properties prop = new Properties();
			prop.load(new FileInputStream(args[0]));
			
			String dbName = prop.getProperty(DB_NAME_PROP);
			String dbUser = prop.getProperty(DB_USER_PROP);
			String dbPassword = prop.getProperty(DB_PASSWORD_PROP);
			String consumerKey = prop.getProperty(CONSUMER_KEY_PROP);
			String consumerSecret = prop.getProperty(CONSUMER_SECRET_PROP);
			
			TwitterRecorderDaemon daemon = new TwitterRecorderDaemon(dbName, dbUser, dbPassword, consumerKey, consumerSecret);
			daemon.init(null);
			daemon.start();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
