# flume-ibmmq-source-sink
Flume Source and Sink for IBM MQ Local QMGR using Bindings

#####Compile with Maven:

First you must obtain a copy of IBM MQ. This can be done via IBM's Trial/Developer Program for MQ - http://www.ibm.com/developerworks/downloads/ws/wmq/

Once you have installed IBM MQ it is necessary to obtain the following jar files below so they can be installed in your local maven repository.

/opt/mqm/java/lib/  
    com.ibm.mq.jar  
    com.ibm.mq.jmqi.jar  
    com.ibm.mq.headers.jar  
  
  They should be named as follows to use the attached pom during the install:  
    <pre><code>
    com.ibm.mq.jar<br>
      &lt;groupId&gt;com.ibm&lt;/groupId&gt;<br>  
      &lt;artifactId&gt;mq.jar&lt;/artifactId&gt;<br>
    com.ibm.mq.jmqi.jar<br>
      &lt;groupId&gt;com.ibm.mq&lt;/groupId&gt;<br> 
      &lt;artifactId&gt;jmqi&lt;/artifactId&gt;<br>
    com.ibm.mq.headers.jar<br>
      &lt;groupId&gt;com.ibm.mq&lt;/groupId&gt;<br> 
      &lt;artifactId&gt;headers&lt;/artifactId&gt;<br>
    </code></pre>  

To Configure Flume with IBM MQ here is an example of flume-env.sh and flume.conf:

###flume.conf:
#####CONFIGURE TYPES  
agent.sources=avro-collection local-ibm-qmgr-source  
agent.sinks=local-ibm-qmgr-sink hdfs-sink  
agent.channels=ibmMQReadChannel ibmMQWriteChannel  

#####SOURCES  
agent.sources.local-ibm-qmgr-source.channels=ibmMQReadChannel  
agent.sources.local-ibm-qmgr-source.gmoconvert=true  
agent.sources.local-ibm-qmgr-source.qmgr=GSSMQP1  
agent.sources.local-ibm-qmgr-source.queue=GSS.REQUEST.REPLY.QUEUE  
agent.sources.local-ibm-qmgr-source.type=org.apache.flume.ibm.mq.LocalQMGRSource  

agent.sources.avro-collection.bind=0.0.0.0  
agent.sources.avro-collection.channels=ibmMQWriteChannel  
agent.sources.avro-collection.port=6969  
agent.sources.avro-collection.type=avro  

#####CHANNELS  
agent.channels.ibmMQReadChannel.capacity=100000000  
agent.channels.ibmMQReadChannel.checkpointDir=/flumecheckpoints/ibmMQReadChannel_checkpoint  
agent.channels.ibmMQReadChannel.dataDirs=/flumecheckpoints/ibmMQReadChannel_data  
agent.channels.ibmMQReadChannel.keepalive=30  
agent.channels.ibmMQReadChannel.maxFileSize=214643507  
agent.channels.ibmMQReadChannel.transactionCapacity=1000  
agent.channels.ibmMQReadChannel.type=file  

agent.channels.ibmMQWriteChannel.capacity=100000000  
agent.channels.ibmMQWriteChannel.checkpointDir=/flumecheckpoints/ibmMQWriteChannel_checkpoint  
agent.channels.ibmMQWriteChannel.dataDirs=/flumecheckpoints/ibmMQWriteChannel_data  
agent.channels.ibmMQWriteChannel.keepalive=30  
agent.channels.ibmMQWriteChannel.maxFileSize=214643507  
agent.channels.ibmMQWriteChannel.transactionCapacity=1000  
agent.channels.ibmMQWriteChannel.type=file  

#####SINKS  
agent.sinks.local-ibm-qmgr-sink.channel=ibmMQWriteChannel  
agent.sinks.local-ibm-qmgr-sink.mqfmtstring=true  
agent.sinks.local-ibm-qmgr-sink.qmgr=GSSMQP1  
agent.sinks.local-ibm-qmgr-sink.queue=GSS.REQUEST.REPLY.QUEUE  
agent.sinks.local-ibm-qmgr-sink.type=org.apache.flume.ibm.mq.LocalQMGRSink  

agent.sinks.hdfs-sink.channel=ibmMQReadChannel  
agent.sinks.hdfs-sink.hdfs.batchSize=1000  
agent.sinks.hdfs-sink.hdfs.filePrefix=ibmmq  
agent.sinks.hdfs-sink.hdfs.fileSuffix=-flumeagent1  
agent.sinks.hdfs-sink.hdfs.fileType=DataStream  
agent.sinks.hdfs-sink.hdfs.idleTimeout=900  
agent.sinks.hdfs-sink.hdfs.inUsePrefix=_INUSE_  
agent.sinks.hdfs-sink.hdfs.kerberosKeytab=/etc/security/keytabs/flume.service.keytab  
agent.sinks.hdfs-sink.hdfs.kerberosPrincipal=flume/hadoopmgmt.example.com@HDPSVC.HDPUSR.EXAMPLE.COM  
agent.sinks.hdfs-sink.hdfs.path=hdfs:///data/flume/mq  
agent.sinks.hdfs-sink.hdfs.proxyUser=hdpsvcid  
agent.sinks.hdfs-sink.hdfs.rollCount=50  
agent.sinks.hdfs-sink.hdfs.rollInterval=900  
agent.sinks.hdfs-sink.hdfs.rollSize=0  
agent.sinks.hdfs-sink.hdfs.txnEventMax=1000  
agent.sinks.hdfs-sink.hdfs.writeFormat=Text  
agent.sinks.hdfs-sink.type=hdfs  

###flume-env.sh:  
if [ -e "/usr/lib/flume/lib/ambari-metrics-flume-sink.jar" ]; then  
      export FLUME_CLASSPATH=$FLUME_CLASSPATH:/usr/lib/flume/lib/ambari-metrics-flume-sink.jar  
fi  

if [ -e "/var/flumemqm/flume-ibmmq-source-sink-0.0.1-SNAPSHOT.jar" ]; then  
      export FLUME_CLASSPATH=$FLUME_CLASSPATH:/var/flumemqm/flume-ibmmq-source-sink-0.0.1-SNAPSHOT.jar  
fi  

if [ -e "/opt/mqm/java/lib/com.ibm.mq.jar" ]; then  
      . /opt/mqm/bin/setmqenv -s -x 64  
      export FLUME_JAVA_LIBRARY_PATH="/opt/mqm/java/lib64"  
      export FLUME_CLASSPATH=$FLUME_CLASSPATH:/opt/mqm/java/lib/com.ibm.mq.jar  
fi
