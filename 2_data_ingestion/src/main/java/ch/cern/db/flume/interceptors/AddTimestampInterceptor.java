package ch.cern.db.flume.interceptors;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

public class AddTimestampInterceptor implements Interceptor {
	
	public static String DATE_FORMAT = "dd/MM/yyyy HH:mm:ss";
	
	private DateFormat format;
	
	public AddTimestampInterceptor() {
	}

	@Override
	public void initialize() {
		format = new SimpleDateFormat(DATE_FORMAT);
	}

	@Override
	public Event intercept(Event event) {
		String body = new String(event.getBody());
		
		String body_with_timestamp = "[" + format.format(new Date()) + "] " + body;
		
		return EventBuilder.withBody(body_with_timestamp.getBytes(), event.getHeaders());
	}

	@Override
	public List<Event> intercept(List<Event> events) {
		LinkedList<Event> intercepted = new LinkedList<Event>();
		
		for (Event event : events) {
			Event intercepted_event = intercept(event);
			
			if(intercepted_event != null)
				intercepted.add(intercepted_event);
		}
		
		return intercepted;
	}

	@Override
	public void close() {
	}

	/**
	 * Builder which builds new instance of this class
	 */
	public static class Builder implements Interceptor.Builder {

		@Override
		public void configure(Context context) {
		}

		@Override
		public Interceptor build() {
			return new AddTimestampInterceptor();
		}

	}
	
}
