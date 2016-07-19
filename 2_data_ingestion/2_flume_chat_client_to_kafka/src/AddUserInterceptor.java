/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */

import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;

public class AddUserInterceptor implements Interceptor {

	private String username;

	public AddUserInterceptor() {
	}
	
	@Override
	public void initialize() {
		username = System.getProperty("user.name");
	}

	@Override
	public Event intercept(Event event) {
		String body = new String(event.getBody());
		
		String body_with_username = "(" + username + "): " + body;
		
		return EventBuilder.withBody(body_with_username.getBytes(), event.getHeaders());
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
			return new AddUserInterceptor();
		}

	}

}
