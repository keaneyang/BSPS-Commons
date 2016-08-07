 package com.bloom.source.cdc;
 
 import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.intf.IDDLRecord;
import com.google.protobuf.ByteString;
import com.webaction.source.cdc.gpb.GPBCommon;
import com.webaction.source.cdc.gpb.GPBCommon._Token;
import com.webaction.source.cdc.gpb.GPBCommon._Token.Builder;
import com.webaction.source.cdc.gpb.GPBRecord;
import com.webaction.source.cdc.gpb.GPBRecord._DDLRecord;
import com.webaction.source.cdc.gpb.GPBRecord._Record;
import com.webaction.source.cdc.gpb.GPBRecord._Record.RecordType;
import java.io.PrintStream;
import java.util.List;
 
 
 
 
 
 
 
 
 public class CDCDDLRecord
   implements IDDLRecord
 {
   private GPBRecord._DDLRecord gpbDDLRecord = null;
   private GPBRecord._DDLRecord.Builder gpbDDLRecordBuilder = null;
   private CDCRecord cdcRecord = null;
   
   public CDCDDLRecord(CDCRecord record)
   {
     this.cdcRecord = record;
     this.gpbDDLRecordBuilder = GPBRecord._DDLRecord.newBuilder();
   }
   
 
   public CDCDDLRecord()
   {
     this.gpbDDLRecordBuilder = GPBRecord._DDLRecord.newBuilder();
     this.gpbDDLRecord = null;
   }
   
 
 
 
 
   public void setTimestamp(long timestamp)
   {
     this.gpbDDLRecordBuilder.setTimestamp(timestamp);
   }
   
   public long getTimestamp()
   {
     return getGPBDDLRecord().getTimestamp();
   }
   
 
 
 
   public void setPosition(byte[] position)
   {
     ByteString posString = ByteString.copyFrom(position);
     this.gpbDDLRecordBuilder.setPosition(posString);
   }
   
   public byte[] getPosition()
   {
     return getGPBDDLRecord().getPosition().toByteArray();
   }
   
 
 
 
   public void setOperationName(String operationName)
   {
     this.gpbDDLRecordBuilder.setOperationname(operationName);
   }
   
 
   public String getOperationName()
   {
     return getGPBDDLRecord().getOperationname();
   }
   
 
 
 
   public void setCatalogObjectType(String objectType)
   {
     this.gpbDDLRecordBuilder.setObjecttype(objectType);
   }
   
   public String getCatalogObjectType()
   {
     return getGPBDDLRecord().getObjecttype();
   }
   
 
 
 
   public void setCatalogObjectName(String objectName)
   {
     this.gpbDDLRecordBuilder.setObjectname(objectName);
   }
   
 
   public String getCatalogObjectName()
   {
     return getGPBDDLRecord().getObjectname();
   }
   
 
 
 
   public void setCommand(String command)
   {
     this.gpbDDLRecordBuilder.setCommand(command);
   }
   
 
   public String getCommand()
   {
     return getGPBDDLRecord().getCommand();
   }
   
   public void process()
   {
     this.gpbDDLRecord = this.gpbDDLRecordBuilder.build();
   }
   
   public GPBRecord._DDLRecord getGPBDDLRecord()
   {
     return this.gpbDDLRecord;
   }
   
   public void dump()
   {
     System.out.println("Printing DDLRecord");
     System.out.println("<-------------------->");
     
     System.out.println("Timestamp value is : " + getTimestamp());
     System.out.println("Position value is : " + getPosition());
     System.out.println("ObjectName  is : " + getCatalogObjectName());
     System.out.println("ObjectType is : " + getCatalogObjectType());
     System.out.println("OperationName is : " + getOperationName());
     System.out.println("Command is : " + getCommand());
   }
   
 
 
   public void setGPBDDLRecord(GPBRecord._DDLRecord gpbDdlrec)
   {
     this.gpbDDLRecord = gpbDdlrec;
   }
   
 
 
 
 
 
 
 
   public byte[] toBytes()
   {
     GPBRecord._Record.Builder gpbRecordBuilder = GPBRecord._Record.newBuilder();
     
     gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.DDL);
     
 
 
 
     gpbRecordBuilder.setDdlrecord(this.gpbDDLRecordBuilder);
     
     GPBRecord._Record gpbRecord = gpbRecordBuilder.build();
     
 
 
 
 
     return gpbRecord.toByteArray();
   }
   
   public String getParameterValue(String name)
   {
     String value = null;
     List<GPBCommon._Token> tokenList = this.gpbDDLRecord.getTokensList();
     
 
 
     for (int i = 0; i < this.gpbDDLRecord.getTokensCount(); i++) {
       GPBCommon._Token tok = (GPBCommon._Token)tokenList.get(i);
       
       if (tok.getName().equals(name)) {
         return tok.getValue();
       }
     }
     
     return value;
   }
   
 
 
 
 
 
 
   public void setParameterValue(String name, String value)
     throws TransactionManagerException
   {
     GPBCommon._Token.Builder gpbTokenBuilder = this.gpbDDLRecordBuilder.addTokensBuilder();
     
     gpbTokenBuilder.setName(name);
     gpbTokenBuilder.setValue(value);
   }
   
   public static void main(String[] args) {}
 }

