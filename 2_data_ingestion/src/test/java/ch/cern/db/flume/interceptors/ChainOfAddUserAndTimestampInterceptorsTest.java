/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */

package ch.cern.db.flume.interceptors;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;


public class ChainOfAddUserAndTimestampInterceptorsTest {

	@Test
	public void user() {
		
		String message = "This is a message";

		Event event = EventBuilder.withBody(message.getBytes());
		
		AddUserInterceptor userInterceptor = new AddUserInterceptor();
		userInterceptor.initialize();
		Event intercepted_event = userInterceptor.intercept(event);
		
		AddTimestampInterceptor timestampInterceptor = new AddTimestampInterceptor();
		timestampInterceptor.initialize();
		intercepted_event = timestampInterceptor.intercept(intercepted_event);
		
		System.out.println(new String(intercepted_event.getBody()));
		
		String intercepted_event_body = new String(intercepted_event.getBody());
		String timestamp_string = intercepted_event_body.substring(
				intercepted_event_body.indexOf('[') + 1,
				intercepted_event_body.indexOf(']'));
		
		try {
			new SimpleDateFormat(AddTimestampInterceptor.DATE_FORMAT).parse(timestamp_string);
		} catch (ParseException e) {
			Assert.fail();
		}
		
		Assert.assertEquals(
				"(" + System.getProperty("user.name") + "): " + message, 
				new String(intercepted_event.getBody()).substring(intercepted_event_body.indexOf(']') + 2));
	}

}
