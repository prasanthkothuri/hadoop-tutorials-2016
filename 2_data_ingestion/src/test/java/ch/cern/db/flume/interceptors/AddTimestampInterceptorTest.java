package ch.cern.db.flume.interceptors;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

public class AddTimestampInterceptorTest {
	
	@Test
	public void timestamp() {
		
		String message = "This is a message";

		Event event = EventBuilder.withBody(message.getBytes());
		
		AddTimestampInterceptor interceptor = new AddTimestampInterceptor();
		interceptor.initialize();
		
		Event intercepted_event = interceptor.intercept(event);
		String intercepted_event_body = new String(intercepted_event.getBody());
		
		String timestamp_string = intercepted_event_body.substring(
				intercepted_event_body.indexOf('[') + 1,
				intercepted_event_body.indexOf(']'));
		
		try {
			new SimpleDateFormat(AddTimestampInterceptor.DATE_FORMAT).parse(timestamp_string);
		} catch (ParseException e) {
			Assert.fail();
		}
	}
	
	@Test
	public void list() {
		
		String message = "This is a message";

		AddTimestampInterceptor interceptor = new AddTimestampInterceptor();
		interceptor.initialize();
		
		List<Event> events = new LinkedList<>();
		events.add(EventBuilder.withBody(message.getBytes()));
		
		List<Event> intercepted_events = interceptor.intercept(events);
		String intercepted_event_body = new String(intercepted_events.get(0).getBody());
		
		String timestamp_string = intercepted_event_body.substring(
				intercepted_event_body.indexOf('[') + 1,
				intercepted_event_body.indexOf(']'));
		
		try {
			new SimpleDateFormat(AddTimestampInterceptor.DATE_FORMAT).parse(timestamp_string);
		} catch (ParseException e) {
			Assert.fail();
		}
	}

}
