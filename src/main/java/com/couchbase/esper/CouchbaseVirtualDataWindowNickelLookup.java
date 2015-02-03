package com.couchbase.esper;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.hook.VirtualDataWindowContext;
import com.espertech.esper.client.hook.VirtualDataWindowLookup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.Set;

/**
 * Created by David on 03/02/2015.
 */
public class CouchbaseVirtualDataWindowNickelLookup implements VirtualDataWindowLookup {
    private static final Log log = LogFactory.getLog(CouchbaseVirtualDataWindowFactory.class);
    private final VirtualDataWindowContext context;

    public CouchbaseVirtualDataWindowNickelLookup(VirtualDataWindowContext context) {
        this.context = context;
        log.debug("CouchbaseVirtualDataWindowNickelLookup(): " + context.toString());
    }

    @Override
    public Set<EventBean> lookup(Object[] keys, EventBean[] eventBeans) {
        // Add code to interrogate lookup-keys here.
        // Create sample event.
        if(keys != null)
            log.debug("Keys: " + keys.toString());
        if(eventBeans != null)
            log.debug("EventBean: " + eventBeans.toString());


        EventBean event = context.getEventFactory().wrap("hello");
        return Collections.singleton(event);
    }


}