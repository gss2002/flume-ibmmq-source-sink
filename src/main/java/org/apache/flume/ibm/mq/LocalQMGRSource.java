/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.ibm.mq;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;

import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mq.MQException;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

public class LocalQMGRSource extends AbstractSource implements Configurable, PollableSource {
    private static final Logger log = LoggerFactory.getLogger(LocalQMGRSource.class);
    private CounterGroup _CounterGroup;
    
    private String qManager;
    private String qName;
    private MQQueueManager qMgr = null;
    private MQQueue queue = null;
    private boolean qGMOConvert = false;
      
    public LocalQMGRSource(){
        _CounterGroup = new CounterGroup();
        MQException.log = null;
    }
                                                                                                                                                 
    @Override
    public PollableSource.Status process() throws EventDeliveryException {

    	Status status = Status.READY;     

		int openOptions = MQConstants.MQOO_INPUT_AS_Q_DEF | MQConstants.MQOO_OUTPUT | MQConstants.MQOO_INQUIRE;
		if (log.isDebugEnabled()) {	
			log.debug("Accessing QMGR="+qManager+" Queue="+qName);
		}
		try {
			if (qMgr != null ) { 
				queue = qMgr.accessQueue(qName, openOptions);
			} else {
				log.error("Resetting Connection to QMGR="+qManager+" QueueName="+qName);
				resetConnection();
				status = Status.BACKOFF;
			}
		} catch (MQException e2) {
			// TODO Auto-generated catch block
			log.error("Cannot access QMGR="+qManager+" Queue="+qName);
			if (queue != null) {
				if (queue.isOpen()) {
					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Still Open - Closing ");
					try {
						queue.close();
					} catch (MQException e) {
					}
				}
			}
			if (qMgr != null) {	
				if (!(qMgr.isConnected() || !(qMgr.isOpen()))) {
					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reseting");
					resetConnection();
				}
			}
			status = Status.BACKOFF;
			log.error(e2.getMessage());
		}

		MQGetMessageOptions gmo = new MQGetMessageOptions();

		// The following is needed to provide backward compatibility with MQ V6 Java code 
		// to properly handle the "format" property of MQHRF2 from JMS messages
		gmo.options = gmo.options + MQConstants.MQGMO_PROPERTIES_FORCE_MQRFH2;
		if (qGMOConvert) {
			gmo.options = gmo.options + MQConstants.MQGMO_CONVERT;
		}
				 ;
		// Get the message off the queue.
     	if (log.isDebugEnabled()) {	
     		log.debug("Getting the message from QMGR="+qManager+" Queue="+qName);
     	}
		int queueDepth = 0;
		try {
			if (queue != null) {
				queueDepth = queue.getCurrentDepth();
				log.info("QMGR="+qManager+" Queue="+qName+" Depth="+queueDepth);
			} else {
				log.info("QMGR="+qManager+" Queue="+qName+" Object Null");
				status = Status.BACKOFF;
			}
		} catch (MQException e1) {
			// TODO Auto-generated catch block
			log.error("Exception - Cannot get QMGR="+qManager+" Queue="+qName+" Depth");
			if (queue != null) {
				if (queue.isOpen()) {
					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Open - Closing Queue");
					try {
						queue.close();
					} catch (MQException e) {
			    	  	log.error("Message Error :: QMGR="+qManager+" Queue="+qName+"");
						log.error("Message Error :: RC="+e.getReason()+" CC="+e.getCompCode());
						log.error("Message Error Cause :: "+e.getCause());
		        		if (log.isDebugEnabled()) {	
		        			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e);
		        		}
					}
				}
			}
			if (qMgr != null) {
				if (!(qMgr.isConnected() || !(qMgr.isOpen()))) {
					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reset");
					resetConnection();
				}
			} else {
				log.error("Exception - QMGR="+qManager+" Queue="+qName+" Null - Reset Connection");
				resetConnection();
			}
    		status = Status.BACKOFF;
    	  	log.error("Message Error :: QMGR="+qManager+" Queue="+qName+"");
			log.error("Message Error :: RC="+e1.getReason()+" CC="+e1.getCompCode());
			log.error("Message Error Cause :: "+e1.getCause());
    		if (log.isDebugEnabled()) {	
    			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e1);		
    		}
    	}
		if (queueDepth != 0 ) {
			for (int i = 0; i < queueDepth; i++) {
				try {
					MQMessage rcvMessage = null;
					String msgText = null;
		        	rcvMessage = new MQMessage();
		        	if (queue.isOpen()) {
		        		queue.get(rcvMessage, gmo);
		        		int strLen = rcvMessage.getMessageLength();
		        		byte[] strData = new byte[strLen];
		        		rcvMessage.readFully(strData);
		        		msgText = new String(strData);
		        		
		        		if (log.isDebugEnabled()) {
		        			log.debug("QMGR="+qManager+" Queue="+qName+" MSG=" + msgText);
		        		}

		        		String msgFormat = rcvMessage.format;
		        		if (log.isDebugEnabled()) {	
		        			log.debug("QMGR="+qManager+" Queue="+qName+" MSG Format=" + msgFormat);
		        		}
					
		        		Event event = new SimpleEvent();
		        			
		        	    try {
				  	  		String data = msgText;
				  	  		Map<String, String> flatHeaders = new HashMap<String, String>(); 
				  	  		flatHeaders.put("DataType", "hdfs");
				  	  		event.setHeaders(flatHeaders);
				  	  		byte[] message = data.getBytes();
				  	  		event.setBody(message);
							
				  	  		getChannelProcessor().processEvent(event);
				  	  		status = Status.READY;				  	  				
				  	  			
			  	  		} catch (Exception e) {
			  	  			e.printStackTrace();
				  	  	}				  	  		
		        	} else {
		        		log.error("QMGR="+qManager+" Queue="+qName+" Closed");
		    			if (queue != null) {
		    				if (queue.isOpen()) {
		    					log.error("QMGR="+qManager+" Queue="+qName+" Open - Closing");
		    					queue.close();
		    				}
		    			}
		    			if (qMgr != null) {
		    				if (!(qMgr.isConnected() || !(qMgr.isOpen()))) {
		    					log.error("QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reset");
		    					resetConnection();
		    				}
		    			} else {
		  					log.error("QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reset");
		    				resetConnection();
		    			}
		        		status = Status.BACKOFF;
		        	}
					
				} catch (Exception ex) {
					if (log.isErrorEnabled())
						log.error(this.getName() + " - Exception - QMGR="+qManager+" Queue="+qName+" while processing event", ex);
			
						// Close the queue
						try {
							if (queue.isOpen()) {
						    	log.error("Exception - QMGR="+qManager+" Queue="+qName+" Open - Closing Queue");
								queue.close();
							}
						} catch (MQException e) {
							// TODO Auto-generated catch block
							log.error("Exception - Closing the queue in Exception Handler");
				    	  	log.error("Message Error :: QMGR="+qManager+" Queue="+qName+"");
    						log.error("Message Error :: RC="+e.getReason()+" CC="+e.getCompCode());
    						log.error("Message Error Cause :: "+e.getCause());
    		        		if (log.isDebugEnabled()) {	
    		        			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e);
    		        		}
    						if (qMgr != null){
    							if (!(qMgr.isConnected() || !(qMgr.isOpen()))) {
    		    					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reset");
									resetConnection();
    							}
    						}
							status = Status.BACKOFF;
						}    
					status = Status.BACKOFF;
				}
        		if (log.isDebugEnabled()) {	
        			log.debug("!!No messages left on QMGR="+qManager+" Queue="+qName+"!!");
        		}
			} 
				
		} else {
			status = Status.BACKOFF;
		}

	    // Close the queue
		if (log.isDebugEnabled()) {	
			log.debug("!!Closing the QMGR="+qManager+" Queue="+qName+"!!");
		}
	    try {
	    	if (queue != null) {
	    		if (queue.isOpen()) {
	    			queue.close();
	    		}
	    	}
	    } catch (MQException e) {
	      	// TODO Auto-generated catch block
	    	  	log.error("Message Error :: Closing QMGR="+qManager+" Queue="+qName+"");
				log.error("Message Error :: RC="+e.getReason()+" CC="+e.getCompCode());
				log.error("Message Error Cause :: "+e.getCause());
        		if (log.isDebugEnabled()) {	
        			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e);
        		}
			  if (qMgr != null){
				  if (!(qMgr.isConnected() || !(qMgr.isOpen()))) {
  					  log.error("Exception - QMGR="+qManager+" Queue="+qName+" Not Connected/Open: Reset");
					  resetConnection();
				  }
			  }
	    	  status = Status.BACKOFF;
	    }
	    return status;     
    }
    
    @Override
    public void configure(Context context) {   
        ensureConfigCompleteness( context );
        log.info("QMGR=" + qManager);   
    }
    
    @Override
    public void start() {
        log.info("Connecting to QMGR="+qManager+" Queue="+qName);
		try {
			qMgr = new MQQueueManager(qManager);
	    	if (qMgr.isConnected()) {
	    		log.info("QMGR="+qManager+" Queue="+qName+" Connected="+qMgr.isConnected());
	    	} else {
	    		log.error("QMGR="+qManager+" Queue="+qName+" Not Connected=false");
	    	}
		} catch (MQException e) {
    	  	log.error("Message Error :: QMGR="+qManager+" Queue="+qName+"");
			log.error("Message Error :: RC="+e.getReason()+" CC="+e.getCompCode());
			log.error("Message Error Cause :: "+e.getCause());
    		if (log.isDebugEnabled()) {	
    			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e);
    		}
		}
    }
    
    @Override
    public synchronized void stop() {
	      log.info("Disconnecting QMGR="+qManager+" Queue="+qName);
	      try {
	    	  if (qMgr != null) {
	    		  qMgr.disconnect();
		    	}		
	      } catch (MQException e) {
	    	log.error("Message Error :: QMGR="+qManager+" Queue="+qName+"");
	    	log.error("Message Error :: RC="+e.getReason()+" CC="+e.getCompCode());
			log.error("Message Error Cause :: "+e.getCause());
    		if (log.isDebugEnabled()) {	
    			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e);
    		}
	      }
	      super.stop();
    }
    
    private void resetConnection(){
        _CounterGroup.incrementAndGet(Constants.COUNTER_EXCEPTION);
        if(log.isWarnEnabled())log.error(this.getName() + "Closing QMGR="+qManager+" Queue="+qName);
	    log.info("Reconnect QMGR="+qManager+" Queue="+qName);
	    try {
	    	log.info("Disconnect QMGR="+qManager+" Queue="+qName);
	    	if (qMgr != null) {
	    		qMgr.disconnect();
	    	} else {
	    		log.error("Null/Not Connected QMGR="+qManager+" Queue="+qName);
	    	}
	    } catch (MQException e) {
    	  	log.error("Message Error :: QMGR="+qManager+" Queue="+qName+"");
			log.error("Message Error :: RC="+e.getReason()+" CC="+e.getCompCode());
			log.error("Message Error Cause :: "+e.getCause());
    		if (log.isDebugEnabled()) {	
    			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e);
    		}
		}
	    log.info("Connecting to QMGR="+qManager+" Queue="+qName);
	    try {
	    	qMgr = new MQQueueManager(qManager);
	    	if (qMgr.isConnected()) {
	    		log.info("QMGR="+qManager+" Connected="+qMgr.isConnected());
	    	} else {
	    		log.error("QMGR="+qManager+" Connected=false");
	    	}
	    } catch (MQException e) {
    	  	log.error("Message Error :: QMGR="+qManager+" Queue="+qName+"");
			log.error("Message Error :: RC="+e.getReason()+" CC="+e.getCompCode());
			log.error("Message Error Cause :: "+e.getCause());
    		if (log.isDebugEnabled()) {	
    			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e);	   
    		}
		}
    }
    
    /**
     * Verify that the Required Configuration exists for IBM MQ Local QMGR Source
     * 
     * @param context
     */
    private void ensureConfigCompleteness( Context context ) {    	
     	if(StringUtils.isEmpty(context.getString(Constants.CONFIG_QMGR )) && StringUtils.isEmpty(context.getString(Constants.CONFIG_QUEUE))) {
    		throw new IllegalArgumentException( "Checking Config Completeness - QueueName and Local QMGR Mising" );
    	} else {
    		qManager = context.getString(Constants.CONFIG_QMGR );
    		qName = context.getString(Constants.CONFIG_QUEUE );
    		qGMOConvert = context.getBoolean(Constants.CONFIG_QGMOCONVERT, false);
    	}
    }
}
