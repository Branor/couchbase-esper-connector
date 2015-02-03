package com.couchbase.esper;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyDescriptor;
import com.espertech.esper.client.hook.*;
import com.sun.deploy.util.StringUtils;
import example.Data;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by David on 03/02/2015.
 */
public class CouchbaseVirtualDataWindowKeyValueLookup implements VirtualDataWindowLookup {
    private static final Log log = LogFactory.getLog(CouchbaseVirtualDataWindowFactory.class);
    private final VirtualDataWindowContext context;
    private final Bucket bucket;
    private final Class type;
    private final ObjectMapper mapper;

    public CouchbaseVirtualDataWindowKeyValueLookup(Bucket bucket, Class type, VirtualDataWindowContext context) {
        this.context = context;
        this.bucket = bucket;

        Class underlyingType = type;
        // If the underlying type is a Map with a "value" key, we'll use the type of the value itself to deserialize the
        // document retrieved from Couchbase.
        if(underlyingType == Map.class) {
            EventPropertyDescriptor epd = context.getEventType().getPropertyDescriptor("value");
            if(epd != null)
                underlyingType = epd.getPropertyType();
        }

        this.type = underlyingType;

        mapper = new ObjectMapper();
        log.debug("CouchbaseVirtualDataWindowLookup(): " + context.toString());
    }

    @Override
    public Set<EventBean> lookup(Object[] keys, EventBean[] eventBeans) {
        if(keys == null || keys.length == 0)
            throw new IllegalArgumentException("Keys for a lookup cannot be null or empty");

        String key = keys[0].toString();
        log.debug("key: " + key);

        if(eventBeans != null)
            log.debug("EventBean: " + eventBeans.toString());

        RawJsonDocument doc = bucket.get(key, RawJsonDocument.class);
        if(doc == null)
            return Collections.EMPTY_SET;

        log.debug(doc.id() + " : " + doc.content());
        Object value = null;
        try {
            value = mapper.readValue(doc.content(), type);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String, Object> eventData = new HashMap<String, Object>();
        eventData.put("key", doc.id());
        eventData.put("value", value);

        EventBean event = context.getEventFactory().wrap(eventData);
        return Collections.singleton(event);
    }
}