package jp;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.prefs.Preferences;

import org.apache.mina.core.buffer.IoBuffer;
import org.red5.client.net.rtmp.RTMPClient;
import org.red5.io.IStreamableFile;
import org.red5.io.ITag;
import org.red5.io.ITagWriter;
import org.red5.io.flv.impl.FLVService;
import org.red5.io.flv.impl.Tag;
import org.red5.io.utils.ObjectMap;
import org.red5.server.api.event.IEvent;
import org.red5.server.api.event.IEventDispatcher;
import org.red5.server.api.service.IPendingServiceCall;
import org.red5.server.api.service.IPendingServiceCallback;
import org.red5.server.net.rtmp.Channel;
import org.red5.server.net.rtmp.RTMPConnection;
import org.red5.server.net.rtmp.codec.RTMP;
import org.red5.server.net.rtmp.event.AudioData;
import org.red5.server.net.rtmp.event.IRTMPEvent;
import org.red5.server.net.rtmp.event.Notify;
import org.red5.server.net.rtmp.event.VideoData;
import org.red5.server.net.rtmp.message.Header;
import org.red5.server.net.rtmp.status.StatusCodes;
import org.red5.server.stream.AbstractClientStream;
import org.red5.server.stream.IStreamData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.auth.RequestToken;
import twitter4j.conf.PropertyConfiguration;


public class Test extends RTMPClient {

	private static final Logger logger = LoggerFactory.getLogger(Test.class);
	private String host = "loungers.flashmediacast.com";
	private String app = "live";
	private String name = "livestream";
	private int port = 1935;
	private Integer streamId;

	public static void main(String[] args) throws Exception {
		StatusListener listener = new StatusListener(){
	        public void onStatus(Status status) {
	            System.out.println(status.getUser().getName() + " : " + status.getText());
	            new Test();
	        }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
			public void onScrubGeo(long arg0, long arg1) {}
			public void onStallWarning(StallWarning arg0) {}
	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }
	    };
	    Preferences userRoot = Preferences.userRoot();
		String token = userRoot.get(PropertyConfiguration.OAUTH_ACCESS_TOKEN, null);
	    String tokenSecret = userRoot.get(PropertyConfiguration.OAUTH_ACCESS_TOKEN_SECRET, null);
	    OAuthAuthorization oAuthAuthorization = new OAuthAuthorization(new PropertyConfiguration(System.getProperties()));
	    if (token == null || tokenSecret == null) {
	    	RequestToken requestToken = oAuthAuthorization.getOAuthRequestToken();
			System.out.println(requestToken.getAuthorizationURL());
	    	String pin = new BufferedReader(new InputStreamReader(System.in)).readLine();
	    	AccessToken accessToken;
	    	if (pin.length() > 0) {
		    	accessToken = oAuthAuthorization.getOAuthAccessToken(requestToken, pin);
	    	} else {
		    	accessToken = oAuthAuthorization.getOAuthAccessToken();
	    	}
		    userRoot.put(PropertyConfiguration.OAUTH_ACCESS_TOKEN, accessToken.getToken());
		    userRoot.put(PropertyConfiguration.OAUTH_ACCESS_TOKEN_SECRET, accessToken.getTokenSecret());
		    userRoot.flush();
	    } else {
	    	oAuthAuthorization.setOAuthAccessToken(new AccessToken(token, tokenSecret));
	    }
	    
	    TwitterStream twitterStream = new TwitterStreamFactory().getInstance(oAuthAuthorization);
	    twitterStream.addListener(listener);
		twitterStream.filter(new FilterQuery().track(new String[]{"@misterpotes #smile"}));
	}
	
	private Test() {
		logger.debug("connecting, host: " + host + ", app: " + app + ", port: " + port);

		IPendingServiceCallback callback = new IPendingServiceCallback() {
			public void resultReceived(IPendingServiceCall call) {
				logger.debug("service call result: " + call);
				if ("connect".equals(call.getServiceMethodName())) {
					Test.this.createStream(this);
				} else if ("createStream".equals(call.getServiceMethodName())) {
					streamId = (Integer) call.getResult();
					play();
				}
			}
		};

		this.connect(host, port, app, callback);
	}
	
	private RTMPConnection conn;
	private ITagWriter writer;

	private int videoTs;
	private int audioTs;

	private long fileSwitch;

	@Override
	public void connectionOpened(RTMPConnection conn, RTMP state) {
		logger.debug("connection opened");
		super.connectionOpened(conn, state);
		this.conn = conn;
		init();
	}

	@Override
	public void connectionClosed(RTMPConnection conn, RTMP state) {
		logger.debug("connection closed");
		super.connectionClosed(conn, state);
		if (writer != null) {
			writer.close();
			writer = null;
		}
//		System.exit(0);
	}

	@Override
	public void createStream(IPendingServiceCallback callback) {
		logger.debug("create stream");
		IPendingServiceCallback wrapper = new CreateStreamCallBack(callback);
		invoke("createStream", null, wrapper);
	}

	@Override
	protected void onInvoke(RTMPConnection conn, Channel channel, Header header, Notify notify, RTMP rtmp) {
		super.onInvoke(conn, channel, header, notify, rtmp);
		ObjectMap<String, String> map = (ObjectMap<String,String>) notify.getCall().getArguments()[0];
		String code = map.get("code");
		logger.debug("Status code: "+code);
		if (StatusCodes.NS_PLAY_STOP.equals(code)) {
			logger.debug("onInvoke, code == NetStream.Play.Stop, disconnecting");
			disconnect();
		}
	}	

	private void init() {
		if (writer != null) writer.close();
		fileSwitch = System.currentTimeMillis() + 10000;
		File file = new File(System.currentTimeMillis()+".flv");
		FLVService flvService = new FLVService();
		flvService.setGenerateMetadata(true);
		try {
			IStreamableFile flv = flvService.getStreamableFile(file);
			writer = flv.getWriter();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void play() {
		logger.debug("createStream result stream id: " + streamId);
		logger.debug("playing video by name: " + name);
		
		play(streamId, name, -1, -1);
	}

	private class CreateStreamCallBack implements IPendingServiceCallback {

		private IPendingServiceCallback wrapped;

		public CreateStreamCallBack(IPendingServiceCallback wrapped) {
			this.wrapped = wrapped;
		}

		public void resultReceived(IPendingServiceCall call) {
			Integer streamIdInt = (Integer) call.getResult();
			if (conn != null && streamIdInt != null) {
				MyNetStream stream = new MyNetStream();
				stream.setConnection(conn);
				stream.setStreamId(streamIdInt.intValue());
				conn.addClientStream(stream);
			}
			wrapped.resultReceived(call);
		}

	}

	private class MyNetStream extends AbstractClientStream implements IEventDispatcher {

		public void close() { }

		public void start() { }

		public void stop() { }

		public void dispatchEvent(IEvent event) {
			if (!(event instanceof IRTMPEvent)) {
				logger.debug("skipping non rtmp event: " + event);
				return;
			}
			IRTMPEvent rtmpEvent = (IRTMPEvent) event;
			if (logger.isDebugEnabled()) {
				logger.debug("rtmp event: " + rtmpEvent.getHeader() + ", "
						+ rtmpEvent.getClass().getSimpleName());
			}
			if (!(rtmpEvent instanceof IStreamData)) {
				logger.debug("skipping non stream data");
				return;
			}
			if (rtmpEvent.getHeader().getSize() == 0) {
				logger.debug("skipping event where size == 0");
				return;
			}
			ITag tag = new Tag();
			tag.setDataType(rtmpEvent.getDataType());
			System.out.println("Type: "+rtmpEvent.getClass().getName());
			if (rtmpEvent instanceof VideoData) {
				videoTs += rtmpEvent.getTimestamp();
				tag.setTimestamp(videoTs);
			} else if (rtmpEvent instanceof AudioData) {
				audioTs += rtmpEvent.getTimestamp();
				tag.setTimestamp(audioTs);
			}
			IoBuffer data = ((IStreamData<?>) rtmpEvent).getData().asReadOnlyBuffer();
			tag.setBodySize(data.limit());
			tag.setBody(data);
			try {
				writer.writeTag(tag);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			if (fileSwitch < System.currentTimeMillis()) {
				Map<String, Object> options = new HashMap<String, Object>();
				options.put("oldStreamName", name);
				options.put("streamName", name);
				options.put("transition", "NetStreamPlayTransitions.STOP");
				play2(streamId, options);
			}
		}
	}

	
}
