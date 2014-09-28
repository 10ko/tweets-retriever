package it.tenko.retriever;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TweetRetriever extends Thread {

	private static String API_KEY = "";
	private static String API_SECRET = "";
	private static String TOKEN = "";
	private static String TOKEN_SECRET = "";
	private static Long TWITTER_ACCOUNT = 0L;

	private static ClientBuilder builder;
	private static Client hosebirdClient;

	private static BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(
			100000);
	private static BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(
			1000);

	private boolean running;
	private static boolean available;

	private static List<String> tweets = Collections
			.synchronizedList(new ArrayList<String>());

	public TweetRetriever() {
		running = false;
		available = true;
	}

	public boolean available() {
		return available;
	}

	@Override
	public void run() {
		while (running) {
			process();
		}

	}

	public static void process() {

		// Attempts to establish a connection.
		hosebirdClient.connect();
		try {
			while (!hosebirdClient.isDone()) {
				String msg;
				msg = getTweetFromJSON(msgQueue.take());

				while (true) {
					if (available) {
						tweets.add(msg);
						break;
					}
					Thread.sleep(100);
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		setupConnection();
		process();
	}

	public void getPropValues() throws IOException {

		Properties prop = new Properties();
		String propFileName = "twitter.properties";

		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
		
		prop.load(inputStream);
		if (inputStream == null) {
			throw new FileNotFoundException("property file '" + propFileName
					+ "' not found in the classpath");
		}

	}

	private static String getTweetFromJSON(String msg) {
		JsonObject tweet = new Gson().fromJson(msg, JsonObject.class);
		//JsonObject userObj = tweet.get("user").getAsJsonObject();
		String text = tweet.get("text").getAsString();
		// String user = userObj.get("screen_name").getAsString();

		return text;
	}

	private static void setupConnection() {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on
		 * expected TPS of your stream
		 */

		/**
		 * Declare the host you want to connect to, the endpoint, and
		 * authentication (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<Long> followings = Lists.newArrayList(TWITTER_ACCOUNT);
		hosebirdEndpoint.followings(followings);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(API_KEY, API_SECRET, TOKEN,
				TOKEN_SECRET);

		builder = new ClientBuilder()
				.name("Hosebird-Client-01")
				// optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue))
				.eventMessageQueue(eventQueue); // optional: use this if you
												// want to process client events

		hosebirdClient = builder.build();
	}

	@Override
	public void start() {
		running = true;
		setupConnection();
		super.start();
	}

	public List<String> getTweets() {
		return tweets;
	}

	public void clearList() {
		tweets.clear();
	}

	public void setAvailable(boolean aval) {
		available = aval;
	}

	public void quit() {
		System.out.println("Quitting.");
		running = false; // Setting running to false ends the loop in run()
		// In case the thread is waiting. . .
		interrupt();
	}

}
