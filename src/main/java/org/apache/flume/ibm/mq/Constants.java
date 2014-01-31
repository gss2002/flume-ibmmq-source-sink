package org.apache.flume.ibm.mq;

public class Constants {
    public final static String COUNTER_NEW_CONNECTION="wmq.newconnection";
    public final static String COUNTER_NEW_CHANNEL="wmq.newchannel";
    public final static String COUNTER_EXCEPTION="wmq.exception";
    public final static String COUNTER_GET="wmq.get";
    public final static String COUNTER_GET_MISS="wmq.getmiss";
    public final static String COUNTER_ACK="wmq.ack";
    public final static String COUNTER_PUBLISH="wmq.publish";
    
    public final static String CONFIG_HOSTNAME="hostname";
    public final static String CONFIG_PORT="port";
    public final static String CONFIG_USERNAME="username";
    public final static String CONFIG_PASSWORD="password";
    public final static String CONFIG_CONNECTIONTIMEOUT="connection_timeout";
    public final static String CONFIG_QUEUE="queue";
    public final static String CONFIG_QMGR="qmgr";
    public final static String CONFIG_QCHANNEL="channel";
    public final static String CONFIG_QGMOCONVERT="gmo_convert";
    public final static String CONFIG_MQFORMATSTRING="mqformat_string";
}
