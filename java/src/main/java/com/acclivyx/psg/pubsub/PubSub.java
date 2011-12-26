package com.acclivyx.psg.pubsub;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.json.JSONArray;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;


public class PubSub implements RedisPubSubListener <String, String> {
	public static final String host = "localhost";
	public static final int port = 6379;
	private final RedisClient client = new RedisClient(host, port);
	private RedisPubSubConnection<String, String> pubsub;
	private RedisConnection<String, String> redis;

	private BlockingQueue<ChannelMessage> channelMessages = new LinkedBlockingQueue<ChannelMessage>();

	public static void main(String[] args) throws Exception {
		PubSub pubSub = new PubSub();
		pubSub.openConnection();
		pubSub.processMessages();
	}

	public PubSub() {
		pubsub = client.connectPubSub();
		pubsub.addListener(this);
		pubsub.subscribe("query");
		pubsub.subscribe("storedProc");
	
	}

	public void processMessages() throws InterruptedException, Exception {
		JdbcQuery jdbcQuery = new JdbcQuery();
		while (true) {
			ChannelMessage cm = channelMessages.take();
			if (cm.channel.equals("query")) {
				JSONArray json = jdbcQuery.performQuery(cm.message);
				redis.publish("query-output", json.toString());
				
			} else if (cm.channel.equals("storedProc")) {
				JSONArray json = jdbcQuery.performRefCursor(cm.message);
				redis.publish("storedProc-output", json.toString());
			}
		}
	}
	public void openConnection() throws Exception {
		redis = client.connect();
		redis.flushall();
	}

	@Override
	public void message(String channel, String message) {
		System.out.println("message:" + channel + " " + message);
		channelMessages.add(new ChannelMessage(channel,message));
	}

	@Override
	public void message(String pattern, String channel, String message) {
	}

	@Override
	public void subscribed(String channel, long count) {
	}

	@Override
	public void psubscribed(String pattern, long count) {
	}

	@Override
	public void unsubscribed(String channel, long count) {
	}

	@Override
	public void punsubscribed(String pattern, long count) {
	}
	private class ChannelMessage {
		private String channel;
		private String message;

		public ChannelMessage(String channel, String message) {
			this.channel = channel;
			this.message = message;
		}
	}
}
