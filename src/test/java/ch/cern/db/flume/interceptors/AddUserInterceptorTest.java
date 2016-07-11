package ch.cern.db.flume.interceptors;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;


public class AddUserInterceptorTest {

	@Test
	public void user() {
		
		String message = "This is a message";

		Event event = EventBuilder.withBody(message.getBytes());
		
		AddUserInterceptor interceptor = new AddUserInterceptor();
		interceptor.initialize();
		
		Event intercepted_event = interceptor.intercept(event);
		
		Assert.assertEquals(
				"(" + System.getProperty("user.name") + "): " + message, 
				new String(intercepted_event.getBody()));
	}

}
