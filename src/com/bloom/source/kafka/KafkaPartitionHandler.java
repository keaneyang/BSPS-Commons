 package com.bloom.source.kafka;
 
 import com.bloom.proc.BaseReader;
import com.bloom.proc.SourceProcess;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.CheckpointDetail;
import com.bloom.recovery.KafkaSourcePosition;
import com.bloom.recovery.Position;
import com.bloom.source.lib.intf.CharParser;
import com.bloom.source.lib.intf.Parser;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.reader.ReaderBase;
import com.bloom.uuid.UUID;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import com.bloom.event.Event;
import com.bloom.source.classloading.ParserLoader;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class KafkaPartitionHandler
   implements Runnable
 {
   public PartitionMetadata partitionMetadata;
   public int partitionId;
   public long kafkaReadOffset;
   public List<String> replicaBrokers;
   public String leaderIp;
   public int leaderPort;
   public SimpleConsumer consumer;
   public String clientName;
   public String topic;
   private boolean recovery = false;
   
 
 
   private int numOfErrors = 0;
   private int leaderLookUp = 0;
   private int blocksize = 0;
   public long waReadOffset;
   public OLVPipedInputStream olvPipedIn;
   public PipedOutputStream pipedOut;
   public Parser parser = null;
   public KafkaProperty localProp;
   private Reader reader;
   private int so_timeout = 0;
   private boolean stopFetching = false;
   
   private boolean sendPosition;
   
   private SourceProcess sourceProcess;
   private UUID sourceRef;
   private String distributionId;
   private static final Logger logger = Logger.getLogger(KafkaPartitionHandler.class);
   private static final long MAX_SLEEP_TIME = 10L;
   
   public KafkaPartitionHandler(String topic, PartitionMetadata parMetadata, long kafkaOffset, long waReadOffset, Property prop) throws Exception {
     this.partitionMetadata = parMetadata;
     this.partitionId = this.partitionMetadata.partitionId();
     this.leaderIp = this.partitionMetadata.leader().host();
     this.leaderPort = this.partitionMetadata.leader().port();
     this.kafkaReadOffset = kafkaOffset;
     this.waReadOffset = waReadOffset;
     this.topic = topic;
     this.blocksize = (prop.blocksize * 1024);
     this.pipedOut = new PipedOutputStream();
     this.olvPipedIn = new OLVPipedInputStream(this.blocksize * 2);
     this.olvPipedIn.connect(this.pipedOut);
     
     this.sourceProcess = ((SourceProcess)prop.propMap.get(BaseReader.SOURCE_PROCESS));
     this.sourceRef = ((UUID)prop.propMap.get(Property.SOURCE_UUID));
     this.distributionId = ((String)prop.propMap.get("distributionId"));
     
     if (this.distributionId == null) {
       this.distributionId = (this.topic + this.partitionId);
     }
     
     this.so_timeout = ((Integer)prop.getMap().get("so_timeout")).intValue();
     UUID sourceRef = (UUID)prop.propMap.get(Property.SOURCE_UUID);
     
     Map<String, Object> localMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
     
     localMap.putAll(prop.propMap);
     localMap.put(Reader.STREAM, this.olvPipedIn);
     localMap.put(Reader.READER_TYPE, Reader.KAFKA_READER);
     localMap.put("TopicName", this.topic);
     localMap.put("PartitionID", Integer.valueOf(this.partitionId));
     
     this.localProp = new KafkaProperty(localMap);
     
     this.parser = ParserLoader.loadParser(this.localProp.propMap, sourceRef);
     if ((!(this.parser instanceof CharParser)) && 
       (this.localProp.getMap().containsKey(Property.CHARSET))) {
       this.localProp.getMap().remove(Property.CHARSET);
     }
     
     this.reader = Reader.createInstance(this.localProp);
     
 
 
 
     CheckpointDetail cp = new CheckpointDetail();
     
     cp.setRecordEndOffset(waReadOffset);
     this.reader.strategy().setCheckPointDetails(cp);
     
     this.replicaBrokers = new ArrayList();
     for (Broker replica : this.partitionMetadata.replicas()) {
       String bokerid = replica.host() + ":" + replica.port();
       this.replicaBrokers.add(bokerid);
     }
     this.clientName = ("Client_" + topic + "_" + this.partitionId);
     this.consumer = new SimpleConsumer(this.partitionMetadata.leader().host(), this.partitionMetadata.leader().port(), this.so_timeout, this.blocksize, this.clientName);
   }
   
   public KafkaPartitionHandler(String topic, PartitionMetadata parMetadata, long kafkaOffset, int blockSize)
   {
     this.partitionMetadata = parMetadata;
     this.topic = topic;
     this.partitionId = parMetadata.partitionId();
     this.replicaBrokers = new ArrayList();
     for (Broker replica : this.partitionMetadata.replicas()) {
       String bokerid = replica.host() + ":" + replica.port();
       this.replicaBrokers.add(bokerid);
     }
     this.kafkaReadOffset = kafkaOffset;
     this.leaderIp = parMetadata.leader().host();
     this.leaderPort = parMetadata.leader().port();
     this.blocksize = blockSize;
     this.clientName = ("Client_" + topic + "_" + this.partitionId);
     this.consumer = new SimpleConsumer(this.partitionMetadata.leader().host(), this.partitionMetadata.leader().port(), this.so_timeout, this.blocksize, this.clientName);
   }
   
 
 
 
 
   public void updatePartitionMetadata(PartitionMetadata pm, long offset)
   {
     this.partitionMetadata = pm;
     this.partitionId = pm.partitionId();
     this.leaderIp = pm.leader().host();
     this.leaderPort = pm.leader().port();
     this.kafkaReadOffset = offset;
     this.numOfErrors = 0;
     this.replicaBrokers.clear();
     this.leaderLookUp = 0;
     for (Broker replica : this.partitionMetadata.replicas()) {
       String bokerid = replica.host() + ":" + replica.port();
       this.replicaBrokers.add(bokerid);
     }
     if (this.consumer != null) {
       this.consumer.close();
       this.consumer = null;
     }
     
     this.consumer = new SimpleConsumer(pm.leader().host(), pm.leader().port(), this.so_timeout, this.blocksize, this.clientName);
   }
   
   public PartitionMetadata getPartitionMetadata()
   {
     return this.partitionMetadata;
   }
   
   public long getParitionReadOffset() {
     return this.kafkaReadOffset;
   }
   
   public void cleanUp() throws IOException {
     this.stopFetching = true;
     if (this.olvPipedIn != null) { this.olvPipedIn.close();this.olvPipedIn = null; }
     if (this.pipedOut != null) { this.pipedOut.close();this.pipedOut = null; }
     if (this.consumer != null) { this.consumer.close();this.consumer = null;
     }
   }
   
   public Reader getReader() { return this.reader; }
   
 
 
 
   public boolean isRecovery()
   {
     return this.recovery;
   }
   
 
 
   public void hasPositioned(boolean recovery)
   {
     this.recovery = recovery;
   }
   
   public void increaseLeaderLookUpCount(int count)
   {
     this.leaderLookUp += count;
   }
   
   public int getLeaderLookUpCount() {
     return this.leaderLookUp;
   }
   
   public int noOfErrors() {
     return this.numOfErrors;
   }
   
   public void increaseErrorCount(int errorCount) {
     this.numOfErrors += errorCount;
   }
   
   public boolean isStopFetching() {
     return this.stopFetching;
   }
   
   public void stopFetching(boolean stop) {
     this.stopFetching = stop;
   }
   
   public void sendPosition(boolean sendPosition) {
     this.sendPosition = sendPosition;
   }
   
   public int getBlocksize() {
     return this.blocksize;
   }
   
   public void run()
   {
     try
     {
       if (logger.isInfoEnabled()) {
         logger.info("(Thread # " + Thread.currentThread().getId() + ") Started the Bloom Kafka Consumer for" + " [" + this.topic + "," + this.partitionId + "] from " + this.leaderIp + ":" + this.leaderPort + ".");
       }
       
       Iterator<Event> eventItr = this.parser.parse(this.reader);
       
       while ((!this.stopFetching) && (!Thread.currentThread().isInterrupted())) {
         try {
           while (eventItr.hasNext()) {
             Event event = (Event)eventItr.next();
             Position outPos = null;
             
             if ((event instanceof WAEvent)) {
               long recordEndOffset = 0L;
               if (((WAEvent)event).metadata != null)
               {
                 Object recordObject = ((WAEvent)event).metadata.get("RecordEnd");
                 if (recordObject != null) recordEndOffset = ((Long)recordObject).longValue();
               }
               if (this.sendPosition) {
                 KafkaSourcePosition sp = new KafkaSourcePosition(this.topic, this.partitionId, recordEndOffset, this.olvPipedIn.getKafkaMessageOffset());
                 outPos = Position.from(this.sourceRef, this.distributionId, sp);
               }
             }
             this.sourceProcess.send(event, 0, outPos);
           }
           
 
 
           Thread.sleep(10L);
         } catch (RuntimeException exp) {
           Throwable t = exp.getCause();
           if ((t instanceof RecordException)) {
             if ((t.getCause() instanceof IOException)) {
               if (this.olvPipedIn != null)
                 if (logger.isInfoEnabled()) {
                   logger.info(exp);
                 }
                 else
                   logger.warn(exp);
               break;
             }
             if (((RecordException)t).type() == RecordException.Type.INVALID_RECORD) {
               if (!Thread.currentThread().isInterrupted())
                 logger.warn(exp.getMessage());
               continue; }
             if (((RecordException)t).type() == RecordException.Type.END_OF_DATASOURCE) {
               break;
             }
             
           }
         }
         catch (Exception exception)
         {
           throw exception;
         }
       }
       return;
     }
     catch (Exception exception) {
       if (((exception instanceof InterruptedException)) || ((exception instanceof InterruptedIOException))) {
         if (logger.isDebugEnabled()) {
           logger.debug("Exception caused by stopping application " + exception);
         }
       } else
         logger.warn(exception);
     } finally {
       try {
         cleanUp();
       } catch (IOException e) {
         if (logger.isTraceEnabled()) {
           logger.trace("Excepition caused during cleanUp. This can be ignored " + e);
         }
       }
     }
   }
 }

