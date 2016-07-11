package ch.cern.db.flume.sources;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.PollableSourceRunner;
import org.junit.Assert;
import org.junit.Test;

public class StreamingAPISourceTest {

	@Test
	public void streaming() {
		
		Context context = new Context();
		context.put(StreamingAPISource.URL_PARAM, "http://stream.meetup.com/2/rsvps");
		context.put(StreamingAPISource.BATCH_SIZE_PARAM, "1");
		
		StreamingAPISource source = new StreamingAPISource();
		source.configure(context);
		
		Map<String, String> channelContext = new HashMap<String, String>();
	    channelContext.put("capacity", "100");
	    channelContext.put("keep-alive", "0"); // for faster tests
	    Channel channel = new MemoryChannel();
	    Configurables.configure(channel, new Context(channelContext));
	    
	    ChannelSelector rcs = new ReplicatingChannelSelector();
	    rcs.setChannels(Collections.singletonList(channel));
	    ChannelProcessor chp = new ChannelProcessor(rcs);
	    source.setChannelProcessor(chp);
	    
	    PollableSourceRunner runner = new PollableSourceRunner();
	    runner.setSource(source);
	    runner.start();
	    
	    int count;
		for(count = 0; count < 5; count++){
		    try {
				Thread.sleep(500);
			} catch (InterruptedException e) {}
		    
		    channel.getTransaction().begin();
		    
		    for (int i = 0; i < 10; i++) {
		    	Event event = channel.take();
		    	if(event != null){
		    		System.out.println(new String(event.getBody()));
		    	}
			}
		    
		    channel.getTransaction().commit();
		    channel.getTransaction().close();
	    }
	    
	    Assert.assertEquals(5, count);
	    
	    runner.stop();
	}
	
}
