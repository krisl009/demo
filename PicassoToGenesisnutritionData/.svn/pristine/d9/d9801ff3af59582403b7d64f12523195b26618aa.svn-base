/*
 * 
 */
package com.cocacola.logging.PTG;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

import org.mule.api.MuleMessage;
import org.mule.api.transport.PropertyScope;

import com.cocacola.logging.generic.GenericUsecaseLog;


/**
 * This is just an example class that demonstrates how
 * the USecase log base class can be extended.
 * 
 *
 */
@SuppressWarnings("rawtypes")
public class PTGUsecaseLog extends GenericUsecaseLog {

	/** The integration identifier. */
	private String integrationId;
	
	/** The integration identifier value. */
	private String integrationIdValue;
	/** The source system identifier. */
	private String sourceSystemId;
	
	/** The source system identifier value. */
	private String sourceSystemValue;
	/** The target system identifier. */
	private String targetSystemId;
	
	/** The target system  value. */
	private String targetSystemValue;
	/** The integration name. */
	private String integrationName;
	
	/** The integration name value. */
	private String integrationNameValue;
	private String correlationID;
	private String resourceType;
	private String resourceName;
	

	/* (non-Javadoc)
	 * @see com.cocacola.logging.generic.GenericUsecaseLog#getUsecaseString(org.mule.api.MuleMessage)
	 */
	public HashMap getUsecaseString(MuleMessage message) {
		
		HashMap<String, String> map = new HashMap<String, String>();
			if(integrationId != null)
				map.put(integrationId, integrationIdValue);
			if(integrationName != null)
				map.put("Integration Name", (String)message.getProperty("integrationName", PropertyScope.OUTBOUND));
			if(sourceSystemId != null)
				map.put(sourceSystemId, sourceSystemValue);
			if(targetSystemId != null)
				map.put(targetSystemId, targetSystemValue);
			if (correlationID != null)
				map.put(correlationID, (String)message.getProperty("correlationID", PropertyScope.SESSION));
			if(resourceType != null)
				map.put(resourceType, (String)message.getProperty("resourceType", PropertyScope.OUTBOUND));
			if(resourceName != null)
				map.put(resourceName, (String)message.getProperty("resourceName", PropertyScope.OUTBOUND));
		return map;
	}
	@Override
	public HashMap getUsecaseLog(MuleMessage message) {
		// TODO Auto-generated method stub
		return getUsecaseString(message);
	}
	public synchronized String getIntegrationId() {
		return integrationId;
	}
	public synchronized void setIntegrationId(String integrationId) {
		this.integrationId = integrationId;
	}
	public synchronized String getIntegrationIdValue() {
		return integrationIdValue;
	}
	public synchronized void setIntegrationIdValue(String integrationIdValue) {
		this.integrationIdValue = integrationIdValue;
	}
	public synchronized String getSourceSystemId() {
		return sourceSystemId;
	}
	public synchronized void setSourceSystemId(String sourceSystemId) {
		this.sourceSystemId = sourceSystemId;
	}
	public synchronized String getSourceSystemValue() {
		return sourceSystemValue;
	}
	public synchronized void setSourceSystemValue(String sourceSystemValue) {
		this.sourceSystemValue = sourceSystemValue;
	}
	public synchronized String getTargetSystemId() {
		return targetSystemId;
	}
	public synchronized void setTargetSystemId(String targetSystemId) {
		this.targetSystemId = targetSystemId;
	}
	public synchronized String getTargetSystemValue() {
		return targetSystemValue;
	}
	public synchronized void setTargetSystemValue(String targetSystemValue) {
		this.targetSystemValue = targetSystemValue;
	}
	public synchronized String getIntegrationName() {
		return integrationName;
	}
	public synchronized void setIntegrationName(String integrationName) {
		this.integrationName = integrationName;
	}
	public synchronized String getIntegrationNameValue() {
		return integrationNameValue;
	}
	public synchronized void setIntegrationNameValue(String integrationNameValue) {
		this.integrationNameValue = integrationNameValue;
	}
	
	public synchronized String getCorrelationID() {
		return correlationID;
	}

	public synchronized void setCorrelationID(String correlationID) {
		this.correlationID = correlationID;
	}
	public synchronized String getResourceType() {
		return resourceType;
	}
	public synchronized void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}
	public synchronized String getResourceName() {
		return resourceName;
	}
	public synchronized void setResourceName(String resourceName) {
		this.resourceName = resourceName;
	}
}
