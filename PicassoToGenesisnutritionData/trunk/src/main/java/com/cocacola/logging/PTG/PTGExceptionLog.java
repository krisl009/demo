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
public class PTGExceptionLog extends GenericExceptionLog {
	private String errorCode;
	private String errorText;
	private String errorType;
	
	
	
	
	/**
	 * Gets the usecase string.
	 *
	 * @param message the message
	 * @return the usecase string
	 */
	public HashMap getExceptionString(MuleMessage message) {
		HashMap map = new HashMap();
		if (errorCode != null)
			map.put(errorCode, message.getProperty("errorCode", PropertyScope.OUTBOUND));
		if (errorText != null)
			map.put(errorText, message.getProperty("errorText", PropertyScope.OUTBOUND));
		if (errorType != null)
			map.put(errorType, message.getProperty("errorType", PropertyScope.OUTBOUND));
		
		return map;
	}

	@Override
	public HashMap getExceptionLog(MuleMessage message) {
		// TODO Auto-generated method stub
		return this.getExceptionString(message);
	}

	public synchronized String getErrorCode() {
		return errorCode;
	}

	public synchronized void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	public synchronized String getErrorText() {
		return errorText;
	}

	public synchronized void setErrorText(String errorText) {
		this.errorText = errorText;
	}

	public synchronized String getErrorType() {
		return errorType;
	}

	public synchronized void setErrorType(String errorType) {
		this.errorType = errorType;
	}

/*	public String getIntegrationIdValue() {
		return integrationIdValue;
	}

	public void setIntegrationIdValue(String integrationIdValue) {
		this.integrationIdValue = integrationIdValue;
	}*/

}
