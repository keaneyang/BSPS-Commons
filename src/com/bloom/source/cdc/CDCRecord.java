 package com.bloom.source.cdc;
 
 import com.bloom.source.cdc.common.CoreException;
import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.intf.IControlRecord;
import com.bloom.source.lib.intf.IDDLRecord;
import com.bloom.source.lib.intf.IDataRecord;
import com.bloom.source.lib.intf.IMetadataRecord;
import com.bloom.source.lib.intf.IRecord;
import com.bloom.source.lib.type.recordtype;
import com.google.protobuf.InvalidProtocolBufferException;
import com.bloom.common.errors.Error;
import com.webaction.source.cdc.gpb.GPBCommon._Error;
import com.webaction.source.cdc.gpb.GPBRecord;
import com.webaction.source.cdc.gpb.GPBRecord._ControlRecord;
import com.webaction.source.cdc.gpb.GPBRecord._DDLRecord;
import com.webaction.source.cdc.gpb.GPBRecord._DataRecord;
import com.webaction.source.cdc.gpb.GPBRecord._MetadataRecord;
import com.webaction.source.cdc.gpb.GPBRecord._Record;
import com.webaction.source.cdc.gpb.GPBRecord._Record.Builder;
import com.webaction.source.cdc.gpb.GPBRecord._Record.RecordType;
import com.webaction.source.cdc.gpb.GPBRecord._StatusRecord;
import com.webaction.source.cdc.gpb.GPBRecord._StatusRecord.StatusType;
import java.util.Hashtable;
import java.util.Map;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class CDCRecord
   implements IRecord
 {
   public static final String META_TABLENAME = "TableName";
   public static final String META_OPERATIONNAME = "OperationName";
   public static final String META_TXNID = "TxnID";
   public static final String META_TXNUSERID = "TxnUserID";
   public static final String META_TIMESTAMP = "TimeStamp";
   public static final String META_ROWID = "ROWID";
   public static final String META_CATALOGOBJECTNAME = "CatalogObjectName";
   public static final String META_CATALOGOBJECTTYPE = "CatalogObjectType";
   public static final String META_AUDITTRAILNAME = "AuditTrailName";
   public static final String META_SESSIONINFO = "SessionInfo";
   public static final String META_RECORDSETID = "RecordSetID";
   public static final String META_TXNNAME = "TransactionName";
   public static final String META_PARENTTXNID = "ParentTxnID";
   public static final String META_SEGMENTNAME = "SegmentName";
   public static final String META_TABLESPACE = "TableSpace";
   public static final String META_THREADID = "ThreadID";
   public static final String META_SERIAL = "Serial";
   public static final String META_SESSION = "Session";
   public static final String META_AUDITSESSIONID = "AuditSessionId";
   public static final String META_RBABLK = "RbaBlk";
   public static final String META_RBASQN = "RbaSqn";
   public static final String META_SQLREDOLENGTH = "SQLRedoLength";
   public static final String META_ROLLBACK = "Rollback";
   public static final String META_SEGMENTTYPE = "SegmentType";
   public static final String META_SCN = "SCN";
   public static final String META_STARTSCN = "STARTSCN";
   public static final String META_COMMITSCN = "COMMITSCN";
   public static final String BYTES_PROCESSED = "BytesProcessed";
   public static final String META_CURRENTSCN = "CURRENTSCN";
   public static final String META_SEQUENCE = "SEQUENCE";
   public static final String IS_PK_UPDATE = "PK_UPDATE";
   GPBRecord._Record gpbRecord = null;
   GPBRecord._Record.Builder gpbRecordBuilder = null;
   recordtype recordType;
   CDCMetaDataRecord cdcMetaDataRecord = null;
   CDCControlRecord cdcControlRecord = null;
   CDCDataRecord cdcDataRecord = null;
   CDCErrorRecord cdcErrorRecord = null;
   CDCDDLRecord cdcDDLRecord = null;
   CDCStatusRecord cdcStatusRecord = null;
   
   String metadataRecordId;
   Map<String, Object> metaDataMapping = null;
   
 
   Logger logger = Logger.getLogger(CDCRecord.class);
   
 
 
 
 
   public CDCRecord()
   {
     this.gpbRecordBuilder = GPBRecord._Record.newBuilder();
     
     this.cdcMetaDataRecord = new CDCMetaDataRecord(this);
     this.cdcControlRecord = new CDCControlRecord(this);
     this.cdcDataRecord = new CDCDataRecord(this);
     this.cdcErrorRecord = new CDCErrorRecord();
     this.cdcDDLRecord = new CDCDDLRecord(this);
     this.cdcStatusRecord = new CDCStatusRecord(this);
     this.metaDataMapping = new Hashtable();
   }
   
 
 
 
 
 
 
 
 
   public void readFromBytes(byte[] recordarray)
     throws CoreException
   {
     String key = null;
     try {
       this.gpbRecord = GPBRecord._Record.parseFrom(recordarray);
     } catch (InvalidProtocolBufferException e) {
       CoreException ce = new CoreException(Error.INVALID_PROTOCOL_BUFFER, e.getMessage());
       throw ce;
     }
     
     if (this.gpbRecord != null)
     {
       if (this.gpbRecord.getRecordtype() == GPBRecord._Record.RecordType.METADATA) {
         setRecordType(recordtype.WA_METADATA);
         
         GPBRecord._MetadataRecord gpbMetaDataRecord = this.gpbRecord.getMetadatarecord();
         this.cdcMetaDataRecord.setGPBMetaDataRecord(gpbMetaDataRecord);
         
 
 
 
         key = this.cdcMetaDataRecord.getMetarecordId();
         setMetadataRecordId(key);
         
 
 
 
 
 
         if (!this.metaDataMapping.containsKey(key)) {
           this.metaDataMapping.put(key, gpbMetaDataRecord);
         } else {
           this.metaDataMapping.remove(key);
           this.metaDataMapping.put(key, gpbMetaDataRecord);
 
         }
         
 
       }
       else if (this.gpbRecord.getRecordtype() == GPBRecord._Record.RecordType.CONTROL) {
         setRecordType(recordtype.WA_CONTROL);
         GPBRecord._ControlRecord gpbControlRecord = this.gpbRecord.getControlrecord();
         this.cdcControlRecord.setGPBControlRecord(gpbControlRecord);
 
       }
       else if (this.gpbRecord.getRecordtype() == GPBRecord._Record.RecordType.DATA) {
         setRecordType(recordtype.WA_DATA);
         
 
         GPBRecord._DataRecord gpbDataRecord = this.gpbRecord.getDatarecord();
         this.cdcDataRecord.setGPBDataRecord(gpbDataRecord);
       }
       else if (this.gpbRecord.getRecordtype() == GPBRecord._Record.RecordType.NOOP) {
         setRecordType(recordtype.WA_NOOP);
         this.logger.debug("NOOP Record denotes no change data during data session and end of meta data in meta data session");
 
 
 
 
 
       }
       else if (this.gpbRecord.getRecordtype() == GPBRecord._Record.RecordType.ERROR) {
         setRecordType(recordtype.WA_ERROR);
         GPBCommon._Error gpbError = this.gpbRecord.getError();
         this.cdcErrorRecord.setGPBErrorRecord(gpbError);
 
       }
       else if (this.gpbRecord.getRecordtype() == GPBRecord._Record.RecordType.DDL) {
         setRecordType(recordtype.WA_DDL);
         GPBRecord._DDLRecord gpbDDLRecord = this.gpbRecord.getDdlrecord();
         this.cdcDDLRecord.setGPBDDLRecord(gpbDDLRecord);
       } else if (this.gpbRecord.getRecordtype() == GPBRecord._Record.RecordType.STATUS) {
         setRecordType(recordtype.WA_STATUS);
         
         GPBRecord._StatusRecord gpbStatusRecord = this.gpbRecord.getStatusrecord();
         this.cdcStatusRecord.setGPBStatusRecord(gpbStatusRecord);
         if ((gpbStatusRecord.getStatustype() == GPBRecord._StatusRecord.StatusType.CDCPROCESS_LAUNCH_FAILURE) || (gpbStatusRecord.getStatustype() == GPBRecord._StatusRecord.StatusType.CDCPROCESS_STOP_FAILURE)) {
           this.cdcErrorRecord.setGPBErrorRecord(gpbStatusRecord.getError());
         }
       }
     }
     
 
 
 
 
     recordarray = null;
   }
   
 
 
   public CDCMetaDataRecord getCDCMetaDataRecord()
   {
     return this.cdcMetaDataRecord;
   }
   
 
 
   public CDCControlRecord getCDCControlRecord()
   {
     return this.cdcControlRecord;
   }
   
 
 
   public CDCDataRecord getCDCDataRecord()
   {
     return this.cdcDataRecord;
   }
   
 
 
 
 
   public CDCErrorRecord getCDCErrorRecord()
   {
     return this.cdcErrorRecord;
   }
   
 
 
 
 
   public CDCStatusRecord getCDCStatusRecord()
   {
     return this.cdcStatusRecord;
   }
   
 
 
 
 
   public void setRecordType(recordtype recordType)
   {
     this.recordType = recordType;
   }
   
 
 
 
 
 
   public recordtype getRecordType()
   {
     return this.recordType;
   }
   
 
 
 
 
   public void dump()
   {
     if (getRecordType() == recordtype.WA_METADATA) {
       getCDCMetaDataRecord().dump();
     } else if (getRecordType() == recordtype.WA_CONTROL) {
       getCDCControlRecord().dump();
     } else if (getRecordType() == recordtype.WA_DATA) {
       getCDCDataRecord().dump();
     } else if (getRecordType() == recordtype.WA_DDL) {
       this.cdcDDLRecord.dump();
     } else if (getRecordType() == recordtype.WA_STATUS) {
       this.cdcStatusRecord.dump();
     }
   }
   
   public IControlRecord getControlRecord() throws TransactionManagerException
   {
     return getCDCControlRecord();
   }
   
   public IDataRecord getDataRecord() throws TransactionManagerException
   {
     return getCDCDataRecord();
   }
   
   public IMetadataRecord getMetaDataRecord()
     throws TransactionManagerException
   {
     return getCDCMetaDataRecord();
   }
   
 
 
 
   public void Process()
   {
     switch (this.recordType)
     {
 
 
     case WA_DATA: 
       getCDCDataRecord().Process();
       
 
 
       this.gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.DATA);
       this.gpbRecordBuilder.setDatarecord(getCDCDataRecord().getGPBDataRecord());
       break;
     
     case WA_STATUS: 
       getCDCStatusRecord().Process();
       this.gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.STATUS);
       this.gpbRecordBuilder.setStatusrecord(getCDCStatusRecord().getGPBStatusRecord());
       break;
     
 
 
     case WA_CONTROL: 
       getCDCControlRecord().Process();
       
 
 
       this.gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.CONTROL);
       this.gpbRecordBuilder.setControlrecord(getCDCControlRecord().getGPBControlRecord());
       
       break;
     
 
 
     case WA_METADATA: 
       getCDCMetaDataRecord().Process();
       
 
 
       this.gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.METADATA);
       this.gpbRecordBuilder.setMetadatarecord(getCDCMetaDataRecord().getGPBMetaDataRecord());
       break;
     
 
 
 
     case WA_ERROR: 
       getCDCErrorRecord().process();
       
 
 
 
       this.gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.ERROR);
       this.gpbRecordBuilder.setError(getCDCErrorRecord().getGPBErrorRecord());
       break;
     case WA_DDL: 
       try {
         CDCDDLRecord cdcDDLRecord = (CDCDDLRecord)getCDCDDLRecord();
         cdcDDLRecord.process();
         this.gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.DDL);
         this.gpbRecordBuilder.setDdlrecord(cdcDDLRecord.getGPBDDLRecord());
       }
       catch (Exception e) {
         throw new RuntimeException(e);
       }
     }
     
     
 
 
 
 
 
     this.gpbRecord = this.gpbRecordBuilder.build();
   }
   
 
 
 
 
   public GPBRecord._Record getGPBRecord()
   {
     return this.gpbRecord;
   }
   
   public GPBRecord._MetadataRecord lookupMetaRecord(String metaDataKey)
   {
     return (GPBRecord._MetadataRecord)this.metaDataMapping.get(metaDataKey);
   }
   
   public void setMetadataRecordId(String metadataRecordId) {
     this.metadataRecordId = metadataRecordId;
   }
   
   public String getMetadataRecordId() {
     return this.metadataRecordId;
   }
   
   public void updateMetaRecordCache(String key, CDCMetaDataRecord metaRecord) {
     if (!this.metaDataMapping.containsKey(key)) {
       this.metaDataMapping.put(key, metaRecord.getGPBMetaDataRecord());
     } else {
       this.metaDataMapping.remove(key);
       this.metaDataMapping.put(key, metaRecord.getGPBMetaDataRecord());
     }
   }
   
 
 
   public IDDLRecord getCDCDDLRecord()
     throws TransactionManagerException
   {
     return this.cdcDDLRecord;
   }
 }
