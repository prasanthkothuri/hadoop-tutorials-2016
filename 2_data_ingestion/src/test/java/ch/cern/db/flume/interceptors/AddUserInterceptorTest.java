/**
 * Copyright (C) 2016, CERN
 * This software is distributed under the terms of the GNU General Public
 * Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".
 * In applying this license, CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */

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
