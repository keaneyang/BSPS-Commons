 package com.bloom.source.cdc;
 
 import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.type.errortype;
import com.google.protobuf.ByteString;
import com.webaction.source.cdc.gpb.GPBCommon;
import com.webaction.source.cdc.gpb.GPBCommon._Error;
import com.webaction.source.cdc.gpb.GPBCommon._Error.Builder;
import com.webaction.source.cdc.gpb.GPBCommon._Error.ErrorType;
import com.webaction.source.cdc.gpb.GPBRecord;
import com.webaction.source.cdc.gpb.GPBRecord._Record;
import com.webaction.source.cdc.gpb.GPBRecord._Record.RecordType;
import java.io.PrintStream;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class CDCErrorRecord
 {
   GPBCommon._Error gpbError = null;
   GPBCommon._Error.Builder gpbErrorBuilder = null;
   
   private errortype errorSeverity = errortype.WA_FATAL;
   
 
 
   public CDCErrorRecord()
   {
     this.gpbErrorBuilder = GPBCommon._Error.newBuilder();
     this.gpbError = null;
   }
   
 
 
 
 
   public GPBCommon._Error getGPBErrorRecord()
   {
     return this.gpbError;
   }
   
 
 
 
 
   public void setGPBErrorRecord(GPBCommon._Error gpbError)
   {
     this.gpbError = gpbError;
   }
   
 
 
 
 
   public void setTimeStamp(long time)
   {
     this.gpbErrorBuilder.setTimestamp(time);
   }
   
 
 
 
 
   public long getTimeStamp()
   {
     return this.gpbError.getTimestamp();
   }
   
 
 
 
 
   public void setPosition(byte[] position)
   {
     ByteString pos = null;
     pos = ByteString.copyFrom(position);
     this.gpbErrorBuilder.setPosition(pos);
   }
   
   public void setLSN(byte[] LSN) throws TransactionManagerException {
     setPosition(LSN);
   }
   
 
 
 
 
   public byte[] getPosition()
   {
     return this.gpbError.getPosition().toByteArray();
   }
   
 
 
 
 
   public void setTransactionId(byte[] transactionID)
   {
     ByteString tid = null;
     tid = ByteString.copyFrom(transactionID);
     this.gpbErrorBuilder.setTransactionId(tid);
   }
   
 
 
 
 
   public byte[] getTransactionId()
   {
     return this.gpbError.getTransactionId().toByteArray();
   }
   
 
 
 
 
   public void setErrorCode(int errorCode)
   {
     this.gpbErrorBuilder.setErrorcode(errorCode);
   }
   
 
 
 
 
   public int getErrorCode()
   {
     return this.gpbError.getErrorcode();
   }
   
 
 
 
 
 
   public void setErrorSeverity(errortype errorSeverity)
   {
     this.errorSeverity = errorSeverity;
     
     switch (errorSeverity)
     {
     case WA_ERROR: 
       this.gpbErrorBuilder.setType(GPBCommon._Error.ErrorType.ERROR);
       break;
     case WA_FATAL: 
       this.gpbErrorBuilder.setType(GPBCommon._Error.ErrorType.FATAL);
       break;
     case WA_INFO: 
       this.gpbErrorBuilder.setType(GPBCommon._Error.ErrorType.INFO);
       break;
     case WA_WARNING: 
       this.gpbErrorBuilder.setType(GPBCommon._Error.ErrorType.WARN);
     }
     
   }
   
 
   public errortype getErrorSeverity()
   {
     int et = getErrorType().getNumber();
     
     switch (et) {
     case 4: 
       return errortype.WA_INFO;
     case 3: 
       return errortype.WA_WARNING;
     case 2: 
       return errortype.WA_ERROR;
     case 1: 
       return errortype.WA_FATAL;
     }
     return errortype.WA_INFO;
   }
   
 
 
 
 
 
   public GPBCommon._Error.ErrorType getErrorType()
   {
     return this.gpbError.getType();
   }
   
 
 
 
 
   public void setErrorMessage(String errorMessage)
   {
     this.gpbErrorBuilder.setErrormessage(errorMessage);
   }
   
 
 
 
 
 
   public String getErrorMessage()
   {
     return this.gpbError.getErrormessage();
   }
   
 
 
 
 
   public void setComponentName(String componentName)
   {
     this.gpbErrorBuilder.setComponentname(componentName);
   }
   
 
 
 
 
   public String getComponentName()
   {
     return this.gpbError.getComponentname();
   }
   
 
 
 
 
   public void setModuleName(String moduleName)
   {
     this.gpbErrorBuilder.setModulename(moduleName);
   }
   
 
 
 
 
 
   public String getModuleName()
   {
     return this.gpbError.getModulename();
   }
   
 
 
 
 
   public void setSubModuleName(String subModuleName)
   {
     this.gpbErrorBuilder.setSubmodulename(subModuleName);
   }
   
 
 
 
 
   public String getSubModuleName()
   {
     if (this.gpbError.hasSubmodulename())
       return this.gpbError.getSubmodulename();
     return null;
   }
   
 
 
 
 
   public void setFileName(String fileName)
   {
     this.gpbErrorBuilder.setFilename(fileName);
   }
   
 
 
 
 
   public String getFileName()
   {
     return this.gpbError.getFilename();
   }
   
 
 
 
 
   public void setLineNo(int lineNo)
   {
     this.gpbErrorBuilder.setLineno(lineNo);
   }
   
 
 
 
 
   public int getLineNo()
   {
     return this.gpbError.getLineno();
   }
   
 
 
 
 
 
 
   public void process()
   {
     this.gpbError = this.gpbErrorBuilder.build();
   }
   
 
   public void dump()
   {
     System.out.println(this.gpbError.toString());
   }
   
 
 
 
   public byte[] toBytes()
   {
     GPBRecord._Record.Builder gpbRecordBuilder = GPBRecord._Record.newBuilder();
     
     gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.ERROR);
     
 
 
 
     gpbRecordBuilder.setError(this.gpbErrorBuilder);
     
     GPBRecord._Record gpbRecord = gpbRecordBuilder.build();
     
 
 
 
 
     return gpbRecord.toByteArray();
   }
 }

