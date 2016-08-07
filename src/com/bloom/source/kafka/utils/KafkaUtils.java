 package com.bloom.source.kafka.utils;
 
 import com.bloom.recovery.Position;
import com.esotericsoftware.kryo.Kryo;
 import com.esotericsoftware.kryo.io.Input;
 import com.esotericsoftware.kryo.io.Output;
 import com.bloom.common.exc.ConnectionException;
import com.bloom.common.exc.MetadataUnavailableException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.common.KafkaException;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 public class KafkaUtils
 {
   private static final Logger logger = Logger.getLogger(KafkaUtils.class);
   
 
 
 
 
 
 
 
 
 
 
 
   public static long getPartitionOffset(String topic, int partition, long whichTime, String ipAddress, int port)
     throws Exception
   {
     String clientName = "Client_" + topic + "_" + partition;
     SimpleConsumer consumer = new SimpleConsumer(ipAddress, port, 30010, 65536, clientName + "OffsetLookUp");
     TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
     Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap();
     
     requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
     kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
     
     OffsetResponse response = consumer.getOffsetsBefore(request);
     
     if (response.hasError()) {
       short code = response.errorCode(topic, partition);
       Throwable cause = ErrorMapping.exceptionFor(code);
       KafkaException k = new KafkaException("Error fetching Offset from the Broker [" + ipAddress + ":" + port + "]", cause);
       logger.warn(k.getCause() + " " + k.getMessage());
       throw k;
     }
     long[] offsets = response.offsets(topic, partition);
     return offsets[0];
   }
   
   public static TopicMetadata lookupTopicMetadata(String ipAddress, int port, String topic, long retryBackoffms) throws Exception
   {
     SimpleConsumer consumer = null;
     TopicMetadata tm = null;
     for (int i = 0; (i < 10) && (tm == null); i++) {
       try {
         consumer = new SimpleConsumer(ipAddress, port, 30010, 65536, "TopicMetadataLookUp");
         List<String> topics = Collections.singletonList(topic);
         TopicMetadataRequest req = new TopicMetadataRequest(topics);
         TopicMetadataResponse resp = consumer.send(req);
         
         List<TopicMetadata> metaData = resp.topicsMetadata();
         
         for (TopicMetadata topicMetadata : metaData) {
           if (topicMetadata.errorCode() > 0) {
             throw new Exception("Failed to fetch topic metadata for the topic " + topic);
           }
           if (topicMetadata.topic().equals(topic)) {
             tm = topicMetadata;
           }
         }
       } catch (Exception e) {
         if ((e instanceof ClosedChannelException)) {
           throw new ConnectionException("Failure in communicating with Broker [" + ipAddress + ":" + port + "] to find the Topic metadata of [" + topic + "].");
         }
         Thread.sleep(retryBackoffms);
       } finally {
         if (consumer != null) consumer.close();
       }
     }
     if (tm == null) {
       throw new MetadataUnavailableException("Please check if the topic " + topic + " exists already.");
     }
     return tm;
   }
   
 
 
 
   public static PartitionMetadata findNewLeader(String topic, int partitionID, List<String> replicaBrokers, String oldLeaderIp, int oldLeaderPort, HashSet<String> fullReplicaList, long retryBackoffms)
     throws Exception
   {
     PartitionMetadata partitionMetadata = null;
     List<String> replicas = new ArrayList();
     replicas.addAll(replicaBrokers);
     
 
 
 
 
     if (replicas.size() == 1) {
       String[] address = ((String)replicas.get(0)).split(":");
       if ((address[0].equals(oldLeaderIp)) && (Integer.parseInt(address[1]) == oldLeaderPort)) {
         for (String brokerid : fullReplicaList) {
           if (!replicas.contains(brokerid)) {
             replicas.add(brokerid);
           }
         }
       }
     }
     for (int i = 0; i < 2; i++) {
       for (String brokerAddress : replicas) {
         boolean retry = false;
         
 
         String[] address = brokerAddress.split(":");
         try
         {
           TopicMetadata topicMetadata = lookupTopicMetadata(address[0], Integer.parseInt(address[1]), topic, retryBackoffms);
           partitionMetadata = getPartitionMetadata(topicMetadata, partitionID);
           if (partitionMetadata == null) {
             retry = true;
           } else if ((partitionMetadata.leader() == null) || (partitionMetadata.leader().host() == null) || (partitionMetadata.leader().port() <= 0)) {
             retry = true;
           } else if ((oldLeaderIp.equalsIgnoreCase(partitionMetadata.leader().host())) && (oldLeaderPort == partitionMetadata.leader().port()) && (i == 0))
           {
 
             if (logger.isDebugEnabled())
               logger.debug("Found the partition metadata. But the Old and new leader port and ip are same - in iteration - " + i + ". So try again");
             retry = true;
           } else {
             return partitionMetadata;
           }
         } catch (Exception e) {}
         if (!(e instanceof ClosedChannelException))
         {
 
 
 
           if (retry) {
             try {
               Thread.sleep(retryBackoffms);
             } catch (InterruptedException ie) {
               if (logger.isDebugEnabled())
                 logger.debug(ie);
             }
           }
         }
       }
       for (String brokerid : fullReplicaList) {
         if (!replicas.contains(brokerid)) {
           replicas.add(brokerid);
         }
       }
     }
     return partitionMetadata;
   }
   
   public static PartitionMetadata getPartitionMetadata(TopicMetadata topicMetadata, int partitionID) {
     for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
       if (partitionMetadata.partitionId() == partitionID) {
         return partitionMetadata;
       }
     }
     return null;
   }
   
   public static String serialize(Object obj)
     throws IOException
   {
     ByteArrayOutputStream out = new ByteArrayOutputStream();
     ObjectOutputStream os = new ObjectOutputStream(out);
     os.writeObject(obj);
     os.flush();
     byte[] bytes = out.toByteArray();
     String str = new String(Base64.encodeBase64(bytes));
     out.close();
     os.close();
     return str;
   }
   
   public static Object deserialize(String str) throws Exception {
     byte[] bytes = Base64.decodeBase64(str);
     ByteArrayInputStream in = new ByteArrayInputStream(bytes);
     ObjectInputStream is = new ObjectInputStream(in);
     Object obj = is.readObject();
     in.close();
     is.close();
     return obj;
   }
   
   public static String kryoSerialize(Position pos) throws IOException {
     ByteArrayOutputStream out = new ByteArrayOutputStream();
     
     Output output = new Output(out);
     Kryo kryo = new Kryo();
     pos.write(kryo, output);
     output.flush();
     byte[] bytes = out.toByteArray();
     String str = new String(Base64.encodeBase64(bytes));
     output.close();
     return str;
   }
   
   public static Position kryoDeserialize(String str) throws IOException {
     byte[] bytes = Base64.decodeBase64(str);
     ByteArrayInputStream in = new ByteArrayInputStream(bytes);
     
     Input input = new Input(in);
     Kryo kryo = new Kryo();
     Position position = new Position();
     position.read(kryo, input);
     input.close();
     return position;
   }
 }

