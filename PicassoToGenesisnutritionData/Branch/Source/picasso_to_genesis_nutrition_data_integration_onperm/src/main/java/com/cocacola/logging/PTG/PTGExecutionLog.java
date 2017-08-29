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
import com.cocacola.logging.generic.GenericExecutionLog;
import com.cocacola.logging.generic.GenericUsecaseLog;


/**
 *This is just an example class that demonstrates how
 *the execution log base class can be extended.
 * 
 *
 */
@SuppressWarnings("rawtypes")
public class PTGExecutionLog extends GenericExecutionLog {
	private String timestamp;
	/*private String correlationID;*/
	private String messageID;
	private String status;
	private String executionPoint;
	/**
	 * Gets the usecase string.
	 *
	 * @param message the message
	 * @return the usecase string
	 */
	public HashMap getExecutionString(MuleMessage message) {
		HashMap map = new HashMap();
		if (timestamp != null)
			map.put(timestamp, message.getProperty("timestamp", PropertyScope.OUTBOUND));
		/*if (correlationID != null)
			map.put(correlationID, message.getProperty("correlationID", PropertyScope.SESSION));*/
		if (messageID != null)
			map.put(messageID, message.getProperty("messageID", PropertyScope.OUTBOUND));
		if (status != null)
			map.put(status, message.getProperty("status", PropertyScope.OUTBOUND));
		if (executionPoint != null)
			map.put(executionPoint, message.getProperty("executionPoint", PropertyScope.OUTBOUND));
		return map;
	}

	@Override
	public HashMap getExecutionLog(MuleMessage message) {
		return this.getExecutionString(message);
	}
	/**
	 * Gets the custom identifier.
	 *
	 * @return the custom identifier
	 */

	public synchronized String getTimestamp() {
		return timestamp;
	}

	public synchronized void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	/*public synchronized String getCorrelationID() {
		return correlationID;
	}

	public synchronized void setCorrelationID(String correlationID) {
		this.correlationID = correlationID;
	}*/

	public synchronized String getMessageID() {
		return messageID;
	}

	public synchronized void setMessageID(String messageID) {
		this.messageID = messageID;
	}

	public synchronized String getStatus() {
		return status;
	}

	public synchronized void setStatus(String status) {
		this.status = status;
	}

	public synchronized String getExecutionPoint() {
		return executionPoint;
	}

	public synchronized void setExecutionPoint(String executionPoint) {
		this.executionPoint = executionPoint;
	}

}
