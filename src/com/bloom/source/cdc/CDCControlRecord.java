 package com.bloom.source.cdc;
 
 import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.intf.IControlRecord;
import com.bloom.source.lib.type.controltype;
import com.google.protobuf.ByteString;
import com.webaction.source.cdc.gpb.GPBCommon;
import com.webaction.source.cdc.gpb.GPBCommon._Token;
import com.webaction.source.cdc.gpb.GPBCommon._Token.Builder;
import com.webaction.source.cdc.gpb.GPBRecord;
import com.webaction.source.cdc.gpb.GPBRecord._ControlRecord;
import com.webaction.source.cdc.gpb.GPBRecord._ControlRecord.ControlType;
import com.webaction.source.cdc.gpb.GPBRecord._Record;
import com.webaction.source.cdc.gpb.GPBRecord._Record.RecordType;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.List;
import org.joda.time.DateTime;
 
 
 
 
 
 
 
 
 
 public class CDCControlRecord
   implements IControlRecord
 {
   GPBRecord._ControlRecord gpbControlRecord = null;
   long timeStamp;
   String position;
   String transactionId;
   String transactionUserId;
   GPBRecord._ControlRecord.ControlType controlType;
   private SimpleDateFormat ft = new SimpleDateFormat("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
   
   // CDC 记录
   CDCRecord cdcRecord = null;
   GPBRecord._ControlRecord.Builder gpbControlRecordBuilder = null;
   // 控制记录主要为Instance 实例和Field 域信息集合
   public CDCControlRecord(CDCRecord record)
   {
     this.cdcRecord = record;
     this.gpbControlRecordBuilder = GPBRecord._ControlRecord.newBuilder();
   }
   
 
 
 
   public CDCControlRecord()
   {
     this.gpbControlRecordBuilder = GPBRecord._ControlRecord.newBuilder();
     this.gpbControlRecord = null;
   }
   
 
 
 
 
   public void setGPBControlRecord(GPBRecord._ControlRecord gpbControlRecord)
   {
     this.gpbControlRecord = gpbControlRecord;
   }
   
 
 
 
 
   public GPBRecord._ControlRecord getGPBControlRecord()
   {
     return this.gpbControlRecord;
   }
   
 
 
 
 
   public DateTime getTimeStamp()
   {
     this.timeStamp = getGPBControlRecord().getTimestamp();
     return new DateTime(this.timeStamp);
   }
   
 
 
 
 
 
   public String getPosition()
   {
     this.position = getGPBControlRecord().getPosition().toStringUtf8();
     return this.position;
   }
   
 
 
   // 获取 TransactionId
 
   public String getTransactionId()
   {
     this.transactionId = getGPBControlRecord().getTransactionId().toStringUtf8();
     return this.transactionId;
   }
   
 
 
   // 获取 TransactionUserId
 
   public String getTransactionUserId()
   {
     this.transactionUserId = getGPBControlRecord().getTransactionUserid().toString();
     return this.transactionUserId;
   }
   
   /*
    * 控制类型包括如下：
    * Begin、Commit、Rollback、Truncate
    */
 
   public void setControlType()
   {
     this.controlType = getGPBControlRecord().getControltype();
   }
   
 
 
 
 
   public controltype getControlType()
   {
     controltype ctlType = controltype.WA_BEGIN;
     
     switch (getGPBControlRecord().getControltype())
     {
     case BEGIN: 
       ctlType = controltype.WA_BEGIN;
       break;
     case COMMIT: 
       ctlType = controltype.WA_COMMIT;
       break;
     case ROLLBACK: 
       ctlType = controltype.WA_ROLLBACK;
       break;
     case TRUNCATE: 
       ctlType = controltype.WA_TRUNCATE;
     }
     
     
 
     return ctlType;
   }
   
 
 
   public void printOperationType()
   {
     setControlType();
     switch (getControlType()) {
     case WA_BEGIN: 
       System.out.println("Operation Type is : BEGIN\n");
       break;
     case WA_COMMIT: 
       System.out.println("Operation Type is : COMMIT\n");
       break;
     case WA_ROLLBACK: 
       System.out.println("Operation Type is : ROLLBACK\n");
       break;
     }
     
   }
   
 
   /*
    * 输出ControlRecord的日志信息：
    * Timestamp、Position、Transaction ID
    */
 
   public void dump()
   {
     System.out.println("Printing ControlRecord");
     System.out.println("<--------------------->");
     System.out.println("Timestamp value is : " + getTimeStamp().toString());
     System.out.println("Position value is : " + getPosition());
     System.out.println("Transaction ID is : " + getTransactionId());
     printOperationType();
   }
   
   public void setTimestamp(long timeStamp)
     throws TransactionManagerException
   {
     this.gpbControlRecordBuilder.setTimestamp(timeStamp);
   }
   // 设置 LSN 号码
   public void setLSN(byte[] LSN) throws TransactionManagerException {
     ByteString posString = ByteString.copyFrom(LSN);
     this.gpbControlRecordBuilder.setPosition(posString);
   }
   // 设置TxnId
   public void setTransactionId(byte[] transID)
     throws TransactionManagerException
   {
     ByteString txnIDString = ByteString.copyFrom(transID);
     this.gpbControlRecordBuilder.setTransactionId(txnIDString);
   }
   
   // 设置ControlType
   public void setControlType(controltype controlType)
     throws TransactionManagerException
   {
     if (controlType == controltype.WA_BEGIN) {
       this.gpbControlRecordBuilder.setControltype(GPBRecord._ControlRecord.ControlType.BEGIN);
     } else if (controlType == controltype.WA_COMMIT) {
       this.gpbControlRecordBuilder.setControltype(GPBRecord._ControlRecord.ControlType.COMMIT);
     } else if (controlType == controltype.WA_ROLLBACK) {
       this.gpbControlRecordBuilder.setControltype(GPBRecord._ControlRecord.ControlType.ROLLBACK);
     } else if (controlType == controltype.WA_TRUNCATE) {
       this.gpbControlRecordBuilder.setControltype(GPBRecord._ControlRecord.ControlType.TRUNCATE);
     }
   }
   
   public void setTransactionUserId(String transUserID) throws TransactionManagerException
   {
     this.gpbControlRecordBuilder.setTransactionUserid(transUserID);
   }
   
   public void setCommand(String command) throws TransactionManagerException
   {
     this.gpbControlRecordBuilder.setCommand(command);
   }
   
 
 
   public void Process()
   {
     this.gpbControlRecord = this.gpbControlRecordBuilder.build();
   }
   
 
   public byte[] toBytes()
   {
     GPBRecord._Record.Builder gpbRecordBuilder = GPBRecord._Record.newBuilder();
     
     gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.CONTROL);
     
 
     // 记录构建器设置控制记录
 
     gpbRecordBuilder.setControlrecord(this.gpbControlRecordBuilder);
     
     GPBRecord._Record gpbRecord = gpbRecordBuilder.build();
     
 
 
     // 记录转换成ByteArray
 
     return gpbRecord.toByteArray();
   }
   
 
   public Object getOperationName()
   {
     String opType = "BEGINs";
     
     switch (getGPBControlRecord().getControltype())
     {
     case BEGIN: 
       opType = "BEGIN";
       break;
     case COMMIT: 
       opType = "COMMIT";
       break;
     case ROLLBACK: 
       opType = "ROLLBACK";
       break;
     default: 
       opType = "TRUNCATE";
     }
     
     
     return opType;
   }
   
   public String getParameterValue(String name)
   {
     String value = null;
     List<GPBCommon._Token> tokenList = this.gpbControlRecord.getTokensList();
     
 
 
 
     for (int i = 0; i < this.gpbControlRecord.getTokensCount(); i++) {
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
     GPBCommon._Token.Builder gpbTokenBuilder = this.gpbControlRecordBuilder.addTokensBuilder();
     
     gpbTokenBuilder.setName(name);
     gpbTokenBuilder.setValue(value);
   }
 }

