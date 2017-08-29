/*
 * 
 */
package com.cocacola.logging.PTG;

import java.util.HashMap;

import org.mule.api.MuleMessage;
import org.mule.api.transport.PropertyScope;

import com.cocacola.logging.generic.GenericPayloadLog;


/**
 * This is just an example class that demonstrates how
 * the paylaod log base class can be extended. 
 *
 */
@SuppressWarnings("rawtypes")
public class PTGPayloadLog extends GenericPayloadLog {

	/* (non-Javadoc)
	 * @see com.cocacola.logging.generic.GenericPayloadLog#getPayloadString(java.lang.Object)
	 */


	@Override
	public HashMap getPayloadLog(MuleMessage message) {
		HashMap map = new HashMap();
		map.put("sapnumber", message.getProperty("sapnumber", PropertyScope.SESSION));
		map.put("machineserial", message.getProperty("machineserial", PropertyScope.SESSION));
		return map;

	}

}
