 package com.bloom.source.cdc;
 
 import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.recordstatus;
import com.webaction.source.cdc.gpb.GPBCommon;
import com.webaction.source.cdc.gpb.GPBCommon._Token;
import com.webaction.source.cdc.gpb.GPBCommon._Token.Builder;
import com.webaction.source.cdc.gpb.GPBRecord;
import com.webaction.source.cdc.gpb.GPBRecord._StatusRecord;
import java.util.List;
 
 public class CDCStatusRecord
 {
   GPBRecord._StatusRecord gpbStatusRecord = null;
   
   CDCRecord cdcRecord = null;
   GPBRecord._StatusRecord.Builder gpbStatusRecordBuilder = null;
   
   public CDCStatusRecord(CDCRecord record)
   {
     this.cdcRecord = record;
     
 
 
 
     this.gpbStatusRecordBuilder = GPBRecord._StatusRecord.newBuilder();
     this.gpbStatusRecord = null;
   }
   
 
 
 
 
   public void setGPBStatusRecord(GPBRecord._StatusRecord gpbStatusRecord)
   {
     this.gpbStatusRecord = gpbStatusRecord;
   }
   
   public void setname(String name) throws TransactionManagerException {
     this.gpbStatusRecordBuilder.setMessage1(name);
   }
   
   public void setInteger(int intMsg) throws TransactionManagerException { this.gpbStatusRecordBuilder.setIntegerMessage(intMsg); }
   
   public void setStatusType(recordstatus statustype) throws TransactionManagerException {
     if (statustype == recordstatus.WA_CDCPROCESS_INIT) {
       this.gpbStatusRecordBuilder.setStatustype(com.webaction.source.cdc.gpb.GPBRecord._StatusRecord.StatusType.CDCPROCESS_INIT);
     } else if (statustype == recordstatus.WA_CDCPROCESS_LAUNCH)
       this.gpbStatusRecordBuilder.setStatustype(com.webaction.source.cdc.gpb.GPBRecord._StatusRecord.StatusType.CDCPROCESS_LAUNCH);
   }
   
   public void setParameterValue(String name, String value) throws TransactionManagerException { GPBCommon._Token.Builder gpbTokenBuilder = this.gpbStatusRecordBuilder.addTokensBuilder();
     
     gpbTokenBuilder.setName(name);
     gpbTokenBuilder.setValue(value);
   }
   
   public void dump() { System.out.println("Printing StatusRecord");
     System.out.println("<-------------------->");
   }
   
   public void Process()
   {
     this.gpbStatusRecord = this.gpbStatusRecordBuilder.build();
     clear();
   }
   
   public void clear() {
     this.gpbStatusRecordBuilder.clear();
   }
   
   public GPBRecord._StatusRecord getGPBStatusRecord() {
     return this.gpbStatusRecord;
   }
   
   public GPBCommon._Token getParameters(int index)
   {
     String value = null;
     List<GPBCommon._Token> tokenList = this.gpbStatusRecord.getTokensList();
     GPBCommon._Token tok = (GPBCommon._Token)tokenList.get(index);
     return tok;
   }
   
   public int getParameterCount() {
     return this.gpbStatusRecord.getTokensCount();
   }
   
   public String getName() {
     return this.gpbStatusRecord.getMessage1();
   }
   
   public int getIntegerMessage() {
     return this.gpbStatusRecord.getIntegerMessage();
   }
   
   public recordstatus getStatusType()
   {
     switch (getGPBStatusRecord().getStatustype())
     {
     case CDCPROCESS_INIT: 
       return recordstatus.WA_CDCPROCESS_INIT;
     case CDCPROCESS_LAUNCH: 
       return recordstatus.WA_CDCPROCESS_LAUNCH;
     case CDCPROCESS_LAUNCH_FAILURE: 
       return recordstatus.WA_CDCPROCESS_LAUNCH_FAILURE;
     case CDCPROCESS_STOP: 
       return recordstatus.WA_STOP_CDCPROCESS;
     case CDCPROCESS_STOP_FAILURE: 
       return recordstatus.WA_STOP_CDCPROCESS_FAILURE;
     }
     
     return null;
   }
   
   public String getTokenName(int index) {
     GPBCommon._Token tok = this.gpbStatusRecord.getTokens(index);
     return tok.getName();
   }
   
   public String getTokenValue(int index) {
     GPBCommon._Token tok = this.gpbStatusRecord.getTokens(index);
     return tok.getValue();
   }
   
   public String getProcessId(int index) {
     return this.gpbStatusRecord.getProcessId(index);
   }
   
   public List<GPBCommon._Token> getTokens() {
     return this.gpbStatusRecord.getTokensList();
   }
 }

