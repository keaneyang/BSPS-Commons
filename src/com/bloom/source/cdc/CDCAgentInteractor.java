 package com.bloom.source.cdc;
 
 import com.bloom.source.cdc.common.CDCException;
import com.bloom.source.cdc.common.Session;
import com.bloom.source.lib.constant.CDCConstant;
import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.intf.ISession;
import com.bloom.source.lib.type.messagetype;
import com.bloom.source.lib.type.sessiontype;
import com.bloom.source.lib.type.sourcetype;
import com.bloom.common.errors.Error;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
 
 
 
 
 /*
  * CDC 代理交互器
  */
 
 public class CDCAgentInteractor
 {
   CDCMessage cdcMessage;											// CDC 消息
   CDCProperty prop;												// CDC 属性
   Logger logger = Logger.getLogger(CDCAgentInteractor.class);
   byte[] responseFromAgent;										// 从代理端流入的字节流
   byte[] requestBuffer; byte[] responseBuffer = new byte['Ѐ'];
   ByteBuffer byteBuffer;
   boolean connectable = false; boolean hasMetaData = false;
   ISession session;
   Socket agentSocket;												// Socket 代理
   InputStream agentResponse;
   OutputStream readerRequest;										// Reader 请求
   Map<String, Object> metadataRecordMap;							// 元数据记录列表
   CDCErrorRecord errorRecord;										// CDC 错误记录
   private boolean sentStopProcess = false;
   
 
 
 
 
   public CDCAgentInteractor(CDCProperty prop, ISession session)
   {
     this.prop = prop;
     this.cdcMessage = new CDCMessage();
     this.errorRecord = new CDCErrorRecord();
     this.metadataRecordMap = new HashMap();
     this.byteBuffer = ByteBuffer.allocateDirect(CDCConstant.WA_MESSAGE_BUFFER);
     this.session = session;
     this.sentStopProcess = false;
   }
   
 
 /*
  * 连接到代理端
  * 首先建立一个代理 Socket, 同时建立输入流和输出流的管道
  */
 
 
   public void connectToAgent()
     throws CDCException
   {
     try
     {
       this.agentSocket = new Socket(this.prop.agentIp, this.prop.agentPortno);
       this.agentResponse = new DataInputStream(this.agentSocket.getInputStream());
       this.readerRequest = new DataOutputStream(this.agentSocket.getOutputStream());
     } catch (UnknownHostException e) {
       CDCException cdcException = new CDCException(Error.INVALID_IP_ADDRESS, e);
       throw cdcException;
     } catch (IOException e) {
       String addMsg = "at " + this.prop.agentIp + ":" + this.prop.agentPortno;
       CDCException cdcException = new CDCException(Error.AGENT_COMMUNICATION_ERROR, e, addMsg);
       throw cdcException;
     }
   }
   
 
 
 
 
 
 /*
  * 发送启动CDC进程的请求, 设置监听器和读取器
  * 在启动CDC时, 建立CDC会话, 处理Seesion和Msg Payload
  */
 
 
   public void sendRequestToStartCDCProcess()
     throws CDCException
   {
     this.cdcMessage.setRequestType(messagetype.START_CDCPROCESS);
     this.cdcMessage.setCDCProcessType(this.prop.cdcProcessType);
     setListenerDetails(this.cdcMessage);
     setReaderDetails(this.cdcMessage);
     this.cdcMessage.setSession((Session)this.session);
     this.cdcMessage.setSesionName(this.prop.sessionName);
     this.logger.debug("mode of cdcprocess " + this.prop.cdcProcessType);
     this.cdcMessage.processSession();
     this.cdcMessage.processMessagePayLoad();
     // 连接到代理端, 发送请求到代理, 关闭 AgentSocket
     connectToAgent();
     sendRequestToAgent(this.cdcMessage.serializeToByteArray().array());
     closeAgentSocket();
     
 
     if (this.logger.isInfoEnabled()) { this.logger.info("CDCReader has sent the request " + this.cdcMessage.getMessageType() + " to Agent");
     }
   }
   
 
   /*
    * 发送结束CDC进程的请求
    * 
    */
 
   public synchronized boolean sendRequestToStopCDCProcess(String[] procId)
     throws CDCException, TransactionManagerException
   {
     if ((this.sentStopProcess) || (this.session.getSessionType() == sessiontype.WA_METADATA_SESSION)) {
       return false;
     }
     
     this.sentStopProcess = true;
     if (procId != null) {
       for (int i = 0; i < procId.length; i++)
       {
         this.cdcMessage.setCDCProcessId(procId[i]);
       }
     } else {
       this.logger.info("CDCReader did not send STOP_CDCPROCESS request to Agent, since the process id list was empty");
       return false;
     }
     this.cdcMessage.setRequestType(messagetype.STOP_CDCPROCESS);
     this.cdcMessage.processListener();
     this.cdcMessage.processMessagePayLoad();
     // 连接到代理端, 发送请求到代理, 关闭 AgentSocket
     connectToAgent();
     sendRequestToAgent(this.cdcMessage.serializeToByteArray().array());
     closeAgentSocket();
     
     if (this.logger.isInfoEnabled()) this.logger.info("CDCReader has sent the request " + this.cdcMessage.getMessageType() + " to Agent");
     return this.sentStopProcess;
   }
   
 
 
 
 
   // 设置监听器进程名称、端口号、主机地址
 
   public void setListenerDetails(CDCMessage cdcMessage)
   {
     cdcMessage.setCDCProcessName(this.prop.listenerName);
     cdcMessage.setWAPortNo(this.prop.portno);
     cdcMessage.setWAHostName(this.prop.ipaddress);
     cdcMessage.processListener();
   }
   
 
 
 
   public void closeAgentSocket()
     throws CDCException
   {
     try
     {
       this.agentSocket.close();
     } catch (IOException e) {
       CDCException cdcException = new CDCException(Error.GENERIC_IO_EXCEPTION, e);
       throw cdcException;
     }
   }
   
 
 
 
 
   // 设置 Reader 的Actor Name、DriverType、UID、SourceType和Actor
 
   private void setReaderDetails(CDCMessage cdcMessage)
   {
     cdcMessage.setActorName(CDCConstant.ACTOR_NAME);
     cdcMessage.setActorDriverType(CDCConstant.DRIVER_TYPE);
     cdcMessage.setActorUID(CDCConstant.ACTOR_UID);
     cdcMessage.setActorSourceType(sourcetype.READER);
     cdcMessage.processActor();
   }
   
 
   // 发送请求到 Agent
 
   public void sendRequestToAgent(byte[] requestToAgent)
     throws CDCException
   {
     try
     {
       this.readerRequest.write(requestToAgent);
     } catch (IOException e) {
       String addMsg = "at " + this.prop.agentIp + ":" + this.prop.agentPortno;
       CDCException cdcException = new CDCException(Error.AGENT_COMMUNICATION_ERROR, e, addMsg);
       throw cdcException;
     }
   }
   
 
 
 
   // 是否为监听器已经连接
 
   public boolean isListenerConnectable()
   {
     return this.connectable;
   }
   
 
 
   // 是否为监听器已经关闭
 
   public boolean isListenerStopped()
   {
     return this.connectable;
   }
 }

