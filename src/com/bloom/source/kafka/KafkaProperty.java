 package com.bloom.source.kafka;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.type.positiontype;

import java.util.Arrays;
import java.util.Map;
 
 
 
 
 public class KafkaProperty
   extends Property
 {
   public static final String PARTITIONIDLIST = "PartitionIDList";
   public static final String STARTOFFSET = "startOffset";
   public static final String KAFKABROKER_ADDRESS = "brokerAddress";
   public static final String KAFKACONFIG = "KafkaConfig";
   public positiontype posType;
   public int positionValue;
   public String[] kafkaBrokerAddress;
   public String partitionList = null;
   
   private String brokerAddressList = null;
   private String[] kafkaBrokerConfigList = null;
   
   public Map<String, Object> getMap() {
     return this.propMap;
   }
   
   public KafkaProperty(Map<String, Object> mp) {
     super(mp);
     
     if (this.propMap.get("brokerAddress") != null) {
       setBrokerAddress(((String)this.propMap.get("brokerAddress")).trim());
       this.kafkaBrokerAddress = this.brokerAddressList.split(",");
     }
     
     if (this.propMap.get("PartitionIDList") != null) {
       this.partitionList = ((String)this.propMap.get("PartitionIDList"));
     } else {
       this.partitionList = System.getProperty("com.bloom.kafkareader.partitionlist");
     }
     
     if (this.propMap.get("startOffset") != null) {
       this.positionValue = Integer.parseInt(this.propMap.get("startOffset").toString());
       
       if (this.positionValue == -1) {
         this.posType = positiontype.WA_POSITION_EOF;
       } else if (this.positionValue == 0) {
         this.posType = positiontype.WA_POSITION_SOF;
       }
     }
     else {
       this.posType = positiontype.WA_POSITION_EOF;
     }
     
     if (this.propMap.get("KafkaConfig") != null) {
       String properties = ((String)this.propMap.get("KafkaConfig")).trim();
       setKafkaBrokerConfigList(properties.split(";"));
     }
   }
   
 
 
 
 
 
   public String getBrokerAddress()
   {
     return this.brokerAddressList;
   }
   
 
 
   public void setBrokerAddress(String brokerAddressList)
   {
     this.brokerAddressList = brokerAddressList;
   }
   
 
 
   public String[] getKafkaBrokerConfigList()
   {
     return this.kafkaBrokerConfigList;
   }
   
 
 
   public void setKafkaBrokerConfigList(String[] kafkaBrokerConfigList)
   {
     if (kafkaBrokerConfigList != null) {
       this.kafkaBrokerConfigList = ((String[])Arrays.copyOf(kafkaBrokerConfigList, kafkaBrokerConfigList.length));
     }
   }
 }

