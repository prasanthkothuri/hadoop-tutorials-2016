import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingAPISource extends AbstractSource implements Configurable, PollableSource {
	
	private static final Logger LOG = LoggerFactory.getLogger(StreamingAPISource.class);

	public static final int BATCH_SIZE_DEFAULT = 10;
	public static final String BATCH_SIZE_PARAM = "batch.size";
	private int batch_size = BATCH_SIZE_DEFAULT;
	
	public static final String URL_PARAM = "url";
	private URL url;
	
	private HttpURLConnection connection;
	private BufferedReader reader;

	private int totalCount;
	
	@Override
	public void configure(Context context) {
		
		String url_string = context.getString(URL_PARAM);
		if(url_string == null)
			throw new ConfigurationException("URL to consume from needs to be configured with " + URL_PARAM);
		try {
			url = new URL(url_string);
		} catch (MalformedURLException e) {
			throw new ConfigurationException("The configured URL to consume from is malformed", e);
		}
		
		try{
			String value = context.getString(BATCH_SIZE_PARAM);
			if(value != null)
				batch_size = Integer.parseInt(value);
		}catch(Exception e){
			throw new FlumeException("Configured value for " + BATCH_SIZE_PARAM + " is not a number", e);
		}
		
		LOG.info("Configured with URL set to (" + url + ") and batch size " + batch_size);
		
		totalCount = 0;
	}
	
	@Override
	public Status process() throws EventDeliveryException {
	
		List<Event> events = new LinkedList<>();
		
    	String line;
		try {
			connect();
			
			LOG.debug("Getting records from API...");
			
			while ((line = reader.readLine()) != null && events.size() < batch_size) {
				LOG.trace("New line: " + line);
				
			    events.add(EventBuilder.withBody(line.getBytes()));
			}
			
			getChannelProcessor().processEventBatch(events);
			
			totalCount += events.size();
			LOG.debug("Total number of records processed: " + totalCount);
			
			if(totalCount % 100 == 0)
				LOG.info("Total number of records processed: " + totalCount);
			
			return Status.READY;
		} catch (IOException e) {
			LOG.error("Error when reading from API", e);
			LOG.warn("Number of records skipped: " + events.size());
			
			if(connection != null)
				connection.disconnect();
			connection = null;
			
			return Status.BACKOFF;
		}
	}

	private void connect() throws IOException {
		if(connection != null)
			return;
		
	    try {
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setRequestProperty("User-Agent", "Mozilla/5.0");
			
			reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			
			LOG.info("Succesfully connected to " + url + " (responde code: " + connection.getResponseCode() + ")");
		} catch (IOException e) {
			LOG.error("Error connecting to API", e);
			
			throw new IOException("Error connecting to API");
		}
		
	}
	
	@Override
	public synchronized void stop() {
		super.stop();
		
		if(connection != null)
			connection.disconnect();
		
		if(reader != null)
			try {
				reader.close();
			} catch (IOException e) {}
	}

	@Override
	public long getBackOffSleepIncrement() {
		return 0;
	}

	@Override
	public long getMaxBackOffSleepInterval() {
		return 0;
	}

}
