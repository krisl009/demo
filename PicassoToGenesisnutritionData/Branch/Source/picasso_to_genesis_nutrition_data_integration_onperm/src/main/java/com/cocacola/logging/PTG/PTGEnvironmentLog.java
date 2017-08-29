/*
 * 
 */
package com.cocacola.logging.PTG;

import java.util.HashMap;
import java.util.UUID;

import org.mule.api.MuleMessage;
import org.mule.api.context.notification.ComponentMessageNotificationListener;
import org.mule.api.context.notification.EndpointMessageNotificationListener;
import org.mule.api.context.notification.MessageProcessorNotificationListener;
import org.mule.api.context.notification.ServerNotification;
import org.mule.api.context.notification.TransactionNotificationListener;
import org.mule.api.transport.PropertyScope;
import org.mule.context.notification.ComponentMessageNotification;
import org.mule.context.notification.EndpointMessageNotification;
import org.mule.context.notification.MessageProcessorNotification;
import org.mule.context.notification.TransactionNotification;
import org.mule.endpoint.DefaultOutboundEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.cocacola.logging.classes.IGenericPayloadLogBean;
import com.cocacola.logging.classes.IGenericUsecaseLogBean;
import com.cocacola.logging.classes.LoggingConstants;
import com.cocacola.logging.generic.GenericEnvironmentLog;
import com.cocacola.logging.generic.GenericExceptionLog;
import com.cocacola.logging.generic.GenericExecutionLog;
import com.cocacola.logging.generic.GenericUsecaseLog;


/**
 *This is just an example class that demonstrates how
 *the execution log base class can be extended.
 * 
 *
 */
@SuppressWarnings("rawtypes")
public class PTGEnvironmentLog extends GenericEnvironmentLog {
	private String ip;
	private String serverName;
	private String domainname;
	
	
	@Override
	public HashMap getEnvironmentLog(ServerNotification notification,
			MuleMessage message) {
		HashMap map = super.getEnvironmentLog(notification);
		if (ip != null)
			map.put(ip, message.getProperty("ip", PropertyScope.OUTBOUND));
		if (serverName != null)
			map.put(serverName, message.getProperty("serverName", PropertyScope.OUTBOUND));
		if (domainname != null)
			map.put(domainname, message.getProperty("domainname", PropertyScope.OUTBOUND));
		
		return map;
	}


	public synchronized String getIp() {
		return ip;
	}


	public synchronized void setIp(String ip) {
		this.ip = ip;
	}


	public synchronized String getServerName() {
		return serverName;
	}


	public synchronized void setServerName(String serverName) {
		this.serverName = serverName;
	}


	public synchronized String getDomainname() {
		return domainname;
	}


	public synchronized void setDomainname(String domainname) {
		this.domainname = domainname;
	}

	
}
