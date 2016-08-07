 package com.bloom.source.cdc;
 
 import com.bloom.source.cdc.common.CDCException;
import com.bloom.source.cdc.common.Session;
import com.bloom.source.lib.constant.CDCConstant;
import com.bloom.source.lib.type.cdcprocesstype;
import com.bloom.source.lib.type.messagetype;
import com.bloom.source.lib.type.sourcetype;
import com.bloom.source.lib.utils.Utils;
import com.webaction.source.cdc.gpb.GPBCommon;
import com.webaction.source.cdc.gpb.GPBCommon._Session;
import com.webaction.source.cdc.gpb.GPBCommon._Session.Builder;
import com.webaction.source.cdc.gpb.GPBCommunicationProtocol;
import com.webaction.source.cdc.gpb.GPBCommunicationProtocol._Actor;
import com.webaction.source.cdc.gpb.GPBCommunicationProtocol._Actor.SourceType;
import com.webaction.source.cdc.gpb.GPBCommunicationProtocol._CDCProcess;
import com.webaction.source.cdc.gpb.GPBCommunicationProtocol._CDCProcess.CDCProcessType;
import com.webaction.source.cdc.gpb.GPBCommunicationProtocol._Message;
import com.webaction.source.cdc.gpb.GPBCommunicationProtocol._Message.MessageType;
import com.webaction.source.cdc.gpb.GPBCommunicationProtocol._MessagePayload;
import java.nio.ByteBuffer;
 
 
 
 
 /*
  * CDC 消息结构定义
  * 通信协议使用Protobuf
  */
 
 
 
 public class CDCMessage
 {
   GPBCommunicationProtocol._Message gpbMessage = null;
   GPBCommunicationProtocol._Message.Builder gpbMessageBuilder = null;
   GPBCommunicationProtocol._CDCProcess gpbcdcProcess = null;
   GPBCommunicationProtocol._CDCProcess.Builder gpbcdcProcessBuilder = null;
   GPBCommunicationProtocol._Actor.Builder gpbActorBuilder = null;
   GPBCommunicationProtocol._Actor gpbActor = null;
   GPBCommunicationProtocol._MessagePayload.Builder gpbMessagePayLoadBuilder = null;
   GPBCommunicationProtocol._MessagePayload gpbMessagePayLoad = null;
   GPBCommon._Session gpbSession = null;
   GPBCommon._Session.Builder gpbSessionBuilder = null;
   byte[] serializedRequest = null;
   
 
 
   // 
 
   ByteBuffer messageBuffer = ByteBuffer.allocate(CDCConstant.WA_MESSAGE_BUFFER);
   
 
 
   public CDCMessage()
   {
     this.gpbMessageBuilder = GPBCommunicationProtocol._Message.newBuilder();
     this.gpbActorBuilder = GPBCommunicationProtocol._Actor.newBuilder();
     this.gpbcdcProcessBuilder = GPBCommunicationProtocol._CDCProcess.newBuilder();
     this.gpbMessagePayLoadBuilder = GPBCommunicationProtocol._MessagePayload.newBuilder();
     this.gpbSessionBuilder = GPBCommon._Session.newBuilder();
   }
   
 
 
 
 
 
 
 
 
   public void setRequestType(messagetype msgType)
   {
     this.gpbMessageBuilder.clearMessagetype();
     switch (msgType)
     {
     case START_CDCPROCESS: 
       this.gpbMessageBuilder.setMessagetype(GPBCommunicationProtocol._Message.MessageType.START_CDCPROCESS);
       break;
     case STOP_CDCPROCESS: 
       this.gpbMessageBuilder.setMessagetype(GPBCommunicationProtocol._Message.MessageType.STOP_CDCPROCESS);
     }
     
   }
   
 
 
 
 
 
   public GPBCommunicationProtocol._Message.MessageType getMessageType()
   {
     return this.gpbMessageBuilder.getMessagetype();
   }
   
 
 
 
 
 
 
 
 
   public void setActorName(String name)
   {
     this.gpbActorBuilder.setName(name);
   }
   
 
 
 
 
   public String getActorName()
   {
     return this.gpbMessageBuilder.getActor().getName();
   }
   
 
 
 
 
   public void setActorUID(String uid)
   {
     this.gpbActorBuilder.setUID(uid);
   }
   
 
 
 
 
   public String getActorUID()
   {
     return this.gpbMessageBuilder.getActor().getUID();
   }
   
 
 
 
 
   public void setActorDriverType(String value)
   {
     this.gpbActorBuilder.setDrivertype(value);
   }
   
 
 
 
 
   public String getActorDriverType()
   {
     return this.gpbMessageBuilder.getActor().getDrivertype();
   }
   
 
 
 
 
 
   public void setActorSourceType(sourcetype sourceType)
   {
     switch (sourceType)
     {
     case AGENT: 
       break;
     
     case LISTENER: 
       break;
     
     case OTHER: 
       this.gpbActorBuilder.setType(GPBCommunicationProtocol._Actor.SourceType.OTHER);
       break;
     
     case READER: 
       this.gpbActorBuilder.setType(GPBCommunicationProtocol._Actor.SourceType.READER);
     }
     
   }
   
 
 
 
 
   public GPBCommunicationProtocol._Actor.SourceType getActorSourceType()
   {
     return this.gpbMessageBuilder.getActor().getType();
   }
   
 
 
 
   GPBCommunicationProtocol._Actor buildActor()
   {
     this.gpbActor = this.gpbActorBuilder.build();
     return this.gpbActor;
   }
   
 
 
 
   public void processActor()
   {
     this.gpbMessageBuilder.setActor(buildActor());
   }
   
 
 
 
 
 
 
 
 
 
   public void setCDCProcessName(String name)
   {
     this.gpbcdcProcessBuilder.setName(name);
   }
   
   public void setCDCProcessType(cdcprocesstype type) {
     switch (type) {
     case MERGED: 
       this.gpbcdcProcessBuilder.setType(GPBCommunicationProtocol._CDCProcess.CDCProcessType.MERGED);
       break;
     case PARALLEL: 
       this.gpbcdcProcessBuilder.setType(GPBCommunicationProtocol._CDCProcess.CDCProcessType.PARALLEL);
       break;
     case SELECTIVE: 
       this.gpbcdcProcessBuilder.setType(GPBCommunicationProtocol._CDCProcess.CDCProcessType.SELECTIVE);
     }
     
   }
   
 
 
 
   public void setWAHostName(String hostName)
   {
     this.gpbcdcProcessBuilder.setWaservername(hostName);
   }
   
 
 
 
 
   public void setWAPortNo(int portNo)
   {
     this.gpbcdcProcessBuilder.setWaportno(portNo);
   }
   
 
 
   public void setCDCProcessId(String pid)
   {
     this.gpbcdcProcessBuilder.addPid(pid);
   }
   
 
 
 
 
 
 
 
   void setCDCProcessTokens(int index) {}
   
 
 
 
 
 
 
   GPBCommunicationProtocol._CDCProcess buildCDCProcess()
   {
     this.gpbcdcProcess = this.gpbcdcProcessBuilder.build();
     return this.gpbcdcProcess;
   }
   
 
 
   public void processListener()
   {
     this.gpbMessagePayLoadBuilder.setCDCProcess(buildCDCProcess());
   }
   
 
 
 
   public void setSession(Session session)
     throws CDCException
   {
     this.gpbSessionBuilder = session.getGPBSession();
   }
   
 
   public void processSession()
   {
     this.gpbSession = this.gpbSessionBuilder.build();
     this.gpbMessagePayLoadBuilder.setSession(this.gpbSession);
   }
   
   public void setSesionName(String name)
   {
     this.gpbSessionBuilder.clearName();
     this.gpbSessionBuilder.setName(name);
   }
   
 
 
   public void processMessagePayLoad()
   {
     this.gpbMessagePayLoad = this.gpbMessagePayLoadBuilder.build();
     this.gpbMessageBuilder.setPayload(this.gpbMessagePayLoad);
   }
   
 
 
 
 
 
 
 
 
 
 
   public ByteBuffer serializeToByteArray()
   {
     this.gpbMessage = getGPBMessage();
     
     int messageSize = this.gpbMessage.getSerializedSize();
     
 
     this.messageBuffer.clear();
     
 
 
 
 
 
     this.messageBuffer.put(Utils.convertIntegerToStringBytes(messageSize));
     
 
 
 
 
     byte[] tempArray = this.gpbMessage.toByteArray();
     
     this.messageBuffer.put(tempArray);
     
 
 
 
     this.messageBuffer.limit(this.messageBuffer.position());
     this.messageBuffer.rewind();
     
 
 
 
 
 
     ByteBuffer tempByteBuffer = ByteBuffer.allocate(this.messageBuffer.limit());
     tempByteBuffer.put(this.messageBuffer);
     tempByteBuffer.rewind();
     
     return tempByteBuffer;
   }
   
 
 
 
 
   public GPBCommunicationProtocol._Message getGPBMessage()
   {
     this.gpbMessage = this.gpbMessageBuilder.build();
     return this.gpbMessage;
   }
   
   public void reset() {
     this.gpbMessageBuilder.clear();
   }
 }

