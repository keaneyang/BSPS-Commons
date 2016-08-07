 package com.bloom.source.cdc;
 
 import com.bloom.proc.SourceProcess;
import com.bloom.source.cdc.common.CDCException;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.type.cdcprocesstype;
import com.bloom.common.errors.Error;
import java.util.Map;
import java.util.TreeMap;
 
 
 
 public class CDCProperty
   extends Property
 {
   int agentPortno = 0;
   
   String agentIp;
   
   int heartbeatInterval;
   
   boolean receiptAck;
   
   String tables;
   String listenerName;
   String sessionName;
   SourceProcess sourceProcess;
   cdcprocesstype cdcProcessType;
   private Map<String, Object> tokenMap;
   
   public CDCProperty(Map<String, Object> mp)
     throws CDCException
   {
     super(mp);
     
     this.tokenMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
     
 
 
 
     this.tokenMap.putAll(mp);
     
 
 
 
 
     this.tokenMap.remove("blocksize");
     this.tokenMap.remove("eof");
     
     if (mp.get("ListenerName") != null) {
       this.listenerName = ((String)mp.get("ListenerName"));
     }
     
 
     if (mp.get("AgentPortNo") != null) {
       this.agentPortno = getInt(mp, "AgentPortNo");
       this.tokenMap.remove("AgentPortNo");
     }
     
     if (mp.get("AgentIpAddress") != null) {
       this.agentIp = ((String)mp.get("AgentIpAddress"));
       this.tokenMap.remove("AgentIpAddress");
     }
     
 
 
 
 
     if (mp.get("Name") != null) {
       this.sessionName = ((String)mp.get("Name"));
       if (this.sessionName.isEmpty()) {
         CDCException cdcException = new CDCException(Error.INVALID_PROPERTY, "Session name cannot be empty");
         throw cdcException;
       }
     }
     
 
 
 
 
 
     if (mp.get("receiptAck") != null) {
       this.receiptAck = getBoolean(mp, "receiptAck");
       this.tokenMap.remove("receiptAck");
       if (this.receiptAck) {
         this.tokenMap.put("LISTENER_ENABLE_RECEIPT_ACK", "true");
       }
     }
     
 
 
 
 
     if (mp.get("heartbeatInterval") != null) {
       this.heartbeatInterval = getInt(mp, "heartbeatInterval");
       this.tokenMap.remove("heartbeatInterval");
       this.tokenMap.put("LISTENER_HEARTBEAT_INTERVAL", String.valueOf(this.heartbeatInterval));
     }
     
 
 
 
 
     if (mp.get("Tables") != null) {
       this.tables = ((String)mp.get("Tables"));
     }
     
 
 
 
 
     this.retryAttempt = 10;
     
     if (mp.get("AuditTrails") != null) {
       String type = (String)mp.get("AuditTrails");
       if (type.equalsIgnoreCase("parallel")) {
         this.cdcProcessType = cdcprocesstype.PARALLEL;
       } else if (type.equalsIgnoreCase("merged")) {
         this.cdcProcessType = cdcprocesstype.MERGED;
       } else {
         this.cdcProcessType = cdcprocesstype.SELECTIVE;
         this.tokenMap.remove("AuditTrails");
         this.tokenMap.put("AUDITTRAILNAME", type);
       }
     }
     
     this.sourceProcess = ((SourceProcess)mp.get("sourceProcess"));
     this.tokenMap.remove("sourceProcess");
   }
   
 
 
 
 
 
   public Map<String, Object> getTokenMap()
   {
     return this.tokenMap;
   }
 }

