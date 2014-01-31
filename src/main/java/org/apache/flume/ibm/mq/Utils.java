package org.apache.flume.ibm.mq;

import org.apache.flume.Context;

public class Utils {
    static final String PREFIX="WMQ";

    public static String getQueueName(Context context) {
        return context.getString(Constants.CONFIG_QUEUE, "");
    }
    
    public static String getQMGR(Context context){
        return context.getString(Constants.CONFIG_QMGR, "");
    }
    
    public static String getHostname(Context context){
        return context.getString(Constants.CONFIG_HOSTNAME, "");
    }
    
    public static String getPort(Context context){
        return context.getString(Constants.CONFIG_PORT, "");
    }
    
    public static String getUsername(Context context){
        return context.getString(Constants.CONFIG_USERNAME, "");
    }
    
    public static String getPassword(Context context){
        return context.getString(Constants.CONFIG_PASSWORD, "");
    }
    
    public static String getConnTimeout(Context context){
        return context.getString(Constants.CONFIG_CONNECTIONTIMEOUT, "");
    }
}
