package com.crowdreplay.recorder;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.ConfigurationBuilder;

public class OAuthTest {

	///////////////////////////////////////////////////////
	// Constants

	
	///////////////////////////////////////////////////////
	// Main Method
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws TwitterException, IOException
	{
		if (args.length < 1)
		{
			System.out.println("Usage: OAuthTest <properties file name>");
			System.exit(-1);
		}
		
		Properties prop = new Properties();
		prop.load(new FileInputStream(args[0]));
		
		String consumerKey = prop.getProperty(TwitterRecorderDaemon.CONSUMER_KEY_PROP);
		String consumerSecret = prop.getProperty(TwitterRecorderDaemon.CONSUMER_SECRET_PROP);
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(true)
		  .setDebugEnabled(true)
		  .setOAuthConsumerKey(consumerKey)
		  .setOAuthConsumerSecret(consumerSecret);
		
		// The factory instance is re-useable and thread safe.
	    Twitter twitter = new TwitterFactory(cb.build()).getInstance();
	    RequestToken requestToken = twitter.getOAuthRequestToken();
	    AccessToken accessToken = null;
	    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	    while (null == accessToken) 
	    {
	    	System.out.println("Open the following URL and grant access to your account:");
		    System.out.println(requestToken.getAuthorizationURL());
		    System.out.print("Enter the OAuth Verifier: ");
		    String verifier = br.readLine();
		    try
		    {
		         if(verifier.length() > 0)
		         {
		        	 accessToken = twitter.getOAuthAccessToken(requestToken, verifier);
		         } 
		    } 
		    catch (TwitterException te) 
		    {
		        if(401 == te.getStatusCode())
		        {
		        	System.out.println("Unable to get the access token.");
		        }
		        else
		        {
		        	te.printStackTrace();
		        }
		    }
	    }

	    //persist to the accessToken for future reference.
	    System.out.println("Id: " + twitter.verifyCredentials().getId());
	    System.out.println("Access Token: " + accessToken.getToken() + " " + accessToken.getTokenSecret());

	    System.exit(0);		
	}
}
