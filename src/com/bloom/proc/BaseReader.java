 package com.bloom.proc;
 
 import com.bloom.intf.Analyzer;
import com.bloom.intf.SourceMetadataProvider;
import com.bloom.proc.SourceProcess;
import com.bloom.recovery.BaseReaderSourcePosition;
import com.bloom.recovery.CheckpointDetail;
import com.bloom.recovery.Path;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.components.Flow;
import com.bloom.source.lib.intf.CharParser;
import com.bloom.source.lib.intf.CheckpointProvider;
import com.bloom.source.lib.intf.Parser;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.usagemetrics.SourceMetricsCollector;
import com.bloom.uuid.UUID;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.InvalidDataException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import com.bloom.event.Event;
import com.bloom.source.classloading.ParserLoader;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Observer;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;
import org.apache.log4j.Logger;
 
 
 public class BaseReader
   extends SourceProcess
   implements Analyzer
 {
   Logger logger = Logger.getLogger(BaseReader.class);
   
 
   public final String PARSER_NAME = "handler";
   public final String BINARY_PARSER_NAME = "BinaryParser";
   public final String XML_PARSER = "XMLParser";
   public final String JSON_PARSER = "JSONParser";
   public final String ISBANK_PARSER = "IsBankParser";
   public final String GG_PARSER = "GGTrailParser";
   public final String SNMP_PARSER = "SNMPParser";
   public final String COLLECTD_PARSER = "CollectdParser";
   public final String VALID_RECORD = "VALID_RECORD";
   public final String INVALID_RECORD = "INVALID_RECORD";
   
   public static String SOURCE_PROCESS = "sourceProcess";
   
   private InputStream reader;
   private Parser parser;
   protected boolean streamType;
   private long lastByteCount = 0L;
   static final String TABLE_NAME = "Tablename";
   private boolean sendPositions = false;
   SourcePosition sourcePosition;
   int noRecordCount = 0;
   int recordCount = 0;
   int sleepTime = 0;
   int maxSleepTime = 100;
   
   boolean breakOnNoRecord;
   boolean closeCalled;
   protected String readerType = "";
   // 记录检查点的细节
   CheckpointDetail recordCheckpoint = null;
   boolean supportCheckpoint;
   boolean isAnalyzer;
   Iterator<Event> iterator;
   private boolean readerParserInitialized = false;
   // 本地属性列表
   private Map<String, Object> localPropertyMap;
   private SourcePosition startPosition;
   
   void retryWait()
   {
     if (this.sleepTime < 10000) {
       this.sleepTime += 1;
     } else if (this.sleepTime < 20000) {
       Thread.yield();
       this.sleepTime += 1;
     } else {
       LockSupport.parkNanos(this.sleepTime);
       if (this.sleepTime < this.maxSleepTime * 1000000) {
         this.sleepTime += 10000;
       }
     }
   }
   
   public synchronized void receiveImpl(int channel, Event out)
     throws Exception
   {
     if (!this.readerParserInitialized) {
       this.parser = createParser(this.localPropertyMap);
       
       if (!(this.parser instanceof CharParser)) {
         this.localPropertyMap.remove(Property.CHARSET);
       }
       
       this.breakOnNoRecord = new Property(this.localPropertyMap).getBoolean("breakonnorecord", false);
       // 记录检查点
       if ((this.startPosition != null) && ((this.startPosition instanceof BaseReaderSourcePosition))) {
         this.recordCheckpoint = ((BaseReaderSourcePosition)this.startPosition).recordCheckpoint;
       }
       else if ((this.logger.isDebugEnabled()) && (this.startPosition != null)) {
         this.logger.debug("Not using startPosition...[" + this.startPosition.toString() + "]");
       }
       
       if (this.recordCheckpoint != null) {
         this.reader = createInputStream(this.localPropertyMap, true);
       } else
         this.reader = createInputStream(this.localPropertyMap);
       if ((this.reader != null) && ((this.reader instanceof Reader))) {
         this.maxSleepTime = ((Reader)this.reader).eofdelay();
       }
       
 
 
       if ((this.reader != null) && ((this.reader instanceof Reader))) {
         if (this.localPropertyMap.get("handler").equals("XMLParser")) {
           Reader tmp = (Reader)this.reader;
           this.reader = Reader.XMLPositioner(tmp);
         } else if (this.localPropertyMap.get("handler").equals("GGTrailParser")) {
           Reader tmp = (Reader)this.reader;
           this.reader = Reader.GGTrailPositioner(tmp);
         }
         ((Reader)this.reader).position(this.recordCheckpoint, true);
       }
       
 
       this.iterator = this.parser.parse(this.reader);
       
       if ((this.parser instanceof CheckpointProvider)) {
         this.supportCheckpoint = true;
       }
       
       if (((this.parser instanceof Observer)) && 
         ((this.reader instanceof Reader))) {
         ((Reader)this.reader).registerObserver(this.parser);
       }
       
       if ((this.parser instanceof Analyzer)) {
         this.isAnalyzer = true;
       }
       
       if ((this.parser == null) || ((!this.isAnalyzer) && (this.iterator == null))) {
         throw new AdapterException("Parser is not instantiated as the handler passed is null");
       }
       // 结束ReaderParser的初始化
       this.readerParserInitialized = true;
     }
     else
     {
       try {
         if (!this.iterator.hasNext()) {
           if (this.breakOnNoRecord)
             throw new RecordException(RecordException.Type.NO_RECORD);
           if (this.testModeLoop) {
             close();
             init(this.prop1Save, this.prop2Save, this.uuidSave, this.distributionIdSave, this.startPositionSave, this.sendPositionsSave, this.flowSave);
             return;
           }
           retryWait();
         } else {
           Position outPos = null;
           Event event = (Event)this.iterator.next();
           if (this.supportCheckpoint) {
             CheckpointDetail recordCheckpoint = ((CheckpointProvider)this.parser).getCheckpointDetail();
             
             long thisByteCount = recordCheckpoint.getBytesRead();
             long sourceBytes = thisByteCount - this.lastByteCount;
             this.lastByteCount = thisByteCount;
             if (((CheckpointProvider)this.parser).getPositionDetail() != null) {
               outPos = ((CheckpointProvider)this.parser).getPositionDetail();
             } else {
               BaseReaderSourcePosition sourcePosition = recordCheckpoint != null ? new BaseReaderSourcePosition(recordCheckpoint) : null;
               outPos = this.sendPositions ? Position.from(this.sourceUUID, this.distributionID, sourcePosition) : null;
               this.sourcePosition = sourcePosition;
             }
             this.metricsCollector.setSourcePosition(this.sourcePosition, this.sourceUUID);
             this.metricsCollector.addSourceBytes(sourceBytes, this.sourceUUID);
           }
           // 发送事件
           send(event, 0, outPos);
           this.recordCount += 1;
           this.sleepTime = 0;
         }
       } catch (RecordException recordExp) {
         if (recordExp.type() == RecordException.Type.NO_RECORD) {
           if (this.breakOnNoRecord)
             throw recordExp;
           if (this.testModeLoop) {
             close();
             init(this.prop1Save, this.prop2Save, this.uuidSave, this.distributionIdSave, this.startPositionSave, this.sendPositionsSave, this.flowSave);
             return;
           }
           retryWait();
         } else if (recordExp.type() != RecordException.Type.INVALID_RECORD)
         {
        	 if (!this.closeCalled)
             throw recordExp;
         }
       } catch (RuntimeException runtimeExp) {
         Throwable t = runtimeExp.getCause();
         if (((t instanceof RecordException)) && (!this.closeCalled)) {
           RecordException rExp = (RecordException)t;
           rExp.errMsg(runtimeExp.getMessage());
           throw rExp;
         }
         if (((t instanceof InvalidDataException)) && (!this.closeCalled)) {
           InvalidDataException ide = (InvalidDataException)t;
           throw ide;
         }
         if (!this.closeCalled)
           throw runtimeExp;
       } catch (Exception exp) {
         exp.printStackTrace();
         throw exp;
       }
     }
   }
   
 
 
 
 
 
   boolean testModeLoop = false;
   Map<String, Object> prop1Save;
   Map<String, Object> prop2Save;
   UUID uuidSave;
   
   public void init(Map<String, Object> properties)
     throws Exception
   {}
   
   public void init(Map<String, Object> prop1, Map<String, Object> prop2, UUID uuid, String distributionId, SourcePosition startPosition, boolean sendPositions, Flow flow) throws Exception
   {
     super.init(prop1, prop2, uuid, distributionId);
     this.closeCalled = false;
     this.sendPositions = sendPositions;
     if (startPosition != null) {
       if (this.logger.isDebugEnabled()) {
         this.logger.debug("Source Position : [" + startPosition.toString() + "]");
       }
     } else if (this.logger.isDebugEnabled()) {
       this.logger.debug("Null Source Position");
     }
     
 
 
 
 
 
     this.localPropertyMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
     this.localPropertyMap.putAll(prop2);
     this.localPropertyMap.putAll(prop1);
     
     customizePropertyMap(this.localPropertyMap);
     
     this.localPropertyMap.put("distributionId", distributionId);
     this.localPropertyMap.put("restartPosition", startPosition);
     this.localPropertyMap.put("sendPositions", Boolean.valueOf(sendPositions));
     
     this.testModeLoop = new Property(this.localPropertyMap).getBoolean("testModeLoop", false);
     
     if (this.testModeLoop) {
       this.prop1Save = prop1;
       this.prop2Save = prop2;
       this.uuidSave = uuid;
       this.distributionIdSave = distributionId;
       this.startPositionSave = startPosition;
       this.sendPositionsSave = sendPositions;
       this.flowSave = flow;
     }
     
     this.startPosition = startPosition;
   }
   
   public void close() throws Exception {
     this.closeCalled = true;
     this.readerParserInitialized = false;
     if (this.parser != null) {
       this.parser.close();
     }
   }
   
   public InputStream createInputStream(Map<String, Object> prop) throws Exception { return createInputStream(prop, false); }
   
   String distributionIdSave;
   SourcePosition startPositionSave;
   boolean sendPositionsSave;
   Flow flowSave;
   public InputStream createInputStream(Map<String, Object> prop, boolean recoveryMode) throws Exception {
     if (!this.readerType.isEmpty())
       prop.put(Property.READER_TYPE, this.readerType);
     return Reader.createInstance(new Property(prop), recoveryMode);
   }
   
   public Parser createParser(Map<String, Object> prop) throws Exception
   {
     return ParserLoader.loadParser(prop, this.sourceUUID);
   }
   
   public Map<String, TypeDefOrName> getMetadata() throws Exception {
     if ((this.parser instanceof SourceMetadataProvider))
       return ((SourceMetadataProvider)this.parser).getMetadata();
     return null;
   }
   
   public String getMetadataKey() {
     if ((this.parser instanceof SourceMetadataProvider))
       return ((SourceMetadataProvider)this.parser).getMetadataKey();
     return null;
   }
   
   public Position getCheckpoint()
   {
     Position result = null;
     if (this.sourceUUID != null) {
       result = new Position(new Path(this.sourceUUID, this.distributionID, this.sourcePosition));
     }
     else if (this.logger.isDebugEnabled()) {
       this.logger.debug("Got null UUID");
     }
     return result;
   }
   
   protected void customizePropertyMap(Map<String, Object> prop) {}
   
   public Map<String, Object> getFileDetails()
   {
     if (this.isAnalyzer)
       return ((Analyzer)this.parser).getFileDetails();
     return null;
   }
   
   public List<Map<String, Object>> getProbableProperties()
   {
     if (this.isAnalyzer)
       return ((Analyzer)this.parser).getProbableProperties();
     return null;
   }
 }
