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


import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;

public class LocalQMGRSink extends AbstractSink implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(LocalQMGRSink.class);
    private CounterGroup _CounterGroup;
    
    private String qManager;
    private String qName;
    private MQQueueManager qMgr = null;
    private MQQueue queue = null;
    private boolean qMqFmtString = false;

    public LocalQMGRSink(){
        _CounterGroup = new CounterGroup();
        MQException.log = null;
    }

	@Override
	public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
    	txn.begin();
        Event event = channel.take();
        if(event == null){
        	txn.commit();
        	txn.close();
            status = Status.BACKOFF;
            log.info("QMGR="+qManager+" Queue="+qName+" - Backoff");
        } else {
        	int openOptions = MQConstants.MQOO_OUTPUT;
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
	    	  	log.error("Message Error :: QMGR="+qManager+" Queue="+qName);
				log.error("Message Error :: RC="+e2.getReason()+" CC="+e2.getCompCode());
				log.error("Message Error Cause :: "+e2.getCause());
        		if (log.isDebugEnabled()) {	
        			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e2);
        		}
        		txn.rollback();
        	    txn.close();
    			log.error("Cannot access QMGR="+qManager+" Queue="+qName);
    			if (queue != null) {
    				if (queue.isOpen()) {
    					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Still Open - Closing ");
    					try {
    						queue.close();
    					} catch (MQException e) {
    			    	  	log.error("Message Error :: QMGR="+qManager+" Queue="+qName);
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
    					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reseting");
						resetConnection();
    				}
    			} else {
					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reseting");
					resetConnection();
    			}
				status = Status.BACKOFF;
	    	  	log.error("Message Error :: QMGR="+qManager+" Queue="+qName);
				log.error("Message Error :: RC="+e2.getReason()+" CC="+e2.getCompCode());
				log.error("Message Error Cause :: "+e2.getCause());
        		if (log.isDebugEnabled()) {	
        			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e2);    
        		}
			}

        	MQPutMessageOptions pmo = new MQPutMessageOptions();
        	pmo.options = pmo.options + MQConstants.MQPMO_NO_SYNCPOINT;
   	
    		if (log.isDebugEnabled()) {	
         		log.debug("Putting the message to QMGR="+qManager+" Queue="+qName);
    		}
			try {
				MQMessage sendMessage = null;
				byte[] mssageOutBytes = event.getBody();
				String messageOut = new String(mssageOutBytes);
				sendMessage = new MQMessage();
	        	if (qMqFmtString) {
	        		sendMessage.format = MQConstants.MQFMT_STRING;
	        	}
				sendMessage.writeString(messageOut);
	    		if (log.isDebugEnabled()) {	
	    			log.debug("QMGR="+qManager+" Queue="+qName+" Message: "+messageOut);
	    		}
		        if (queue.isOpen()) {
		        	log.info("Sending Message via QMGR="+qManager+" Queue="+qName);
		        	queue.put(sendMessage, pmo);
		            txn.commit();
		  	  		status = Status.READY;				  	  				
				} else {
	        		txn.rollback();
					log.error("QMGR="+qManager+" Queue="+qName+" - Closed");
					if (queue.isOpen()) {
						log.error("QMGR="+qManager+" Queue="+qName+" - Closing");
						queue.close();
					}
					if (qMgr != null){
						if (!(qMgr.isConnected() || !(qMgr.isOpen()))) {
							log.error("Exception - QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reset");
							resetConnection();
						}
					}
		        	status = Status.BACKOFF;
		        }				
			} catch (Exception ex) {
				if (log.isErrorEnabled())
					log.error(this.getName() + " - Exception - QMGR="+qManager+" Queue="+qName+" thrown while processing event", ex);			
					txn.rollback();
					txn.close();

					try {
						if (queue.isOpen()) {
							log.error("QMGR="+qManager+" Queue="+qName+" - Closing");
							queue.close();
						}
					} catch (MQException e) {
						log.error("Exception: Closing the queue in Exception Handler");
						log.error("Message Error :: QMGR="+qManager+" Queue="+qName);
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
			} finally {				
        		if (log.isDebugEnabled()) {	
        			log.debug("Closing - QMGR="+qManager+" Queue="+qName);
        		}
				try {
					txn.close();
					if (queue.isOpen()) {
						queue.close();
					}
				} catch (MQException e) {
					log.error("Exception Closing Queue");
					log.error("Message Error :: QMGR="+qManager+" Queue="+qName);
					log.error("Message Error :: RC="+e.getReason()+" CC="+e.getCompCode());
					log.error("Message Error Cause :: "+e.getCause());
	        		if (log.isDebugEnabled()) {	
	        			log.debug("QMGR="+qManager+" Queue="+qName+" Error {}",e);
	        		}
					if (qMgr != null) {
						if (!(qMgr.isConnected() || !(qMgr.isOpen()))) {
	    					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reset");
							resetConnection();
						}
					} else {
    					log.error("Exception - QMGR="+qManager+" Queue="+qName+" Not Connected/Open - Reset");
						resetConnection();
					}
					status = Status.BACKOFF;
				}
			}
        }  
		return status;     
    }

    @Override
    public void configure(Context context) {
        ensureConfigCompleteness( context );
        log.info("Configuring queue manager: " + qManager);     
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
     		qMqFmtString = context.getBoolean(Constants.CONFIG_MQFORMATSTRING, false);
     	}    
    }
}

