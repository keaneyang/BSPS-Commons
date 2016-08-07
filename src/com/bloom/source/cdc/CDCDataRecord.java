 package com.bloom.source.cdc;
 
 import com.bloom.source.cdc.common.CDCException;
import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.intf.IDataRecord;
import com.bloom.source.lib.meta.ByteColumn;
import com.bloom.source.lib.meta.Column;
import com.bloom.source.lib.meta.DoubleColumn;
import com.bloom.source.lib.meta.FloatColumn;
import com.bloom.source.lib.meta.IntegerColumn;
import com.bloom.source.lib.meta.LEDateColumn;
import com.bloom.source.lib.meta.LEDatetimeColumn;
import com.bloom.source.lib.meta.LEDoubleColumn;
import com.bloom.source.lib.meta.LEFloatColumn;
import com.bloom.source.lib.meta.LEIntegerColumn;
import com.bloom.source.lib.meta.LELongColumn;
import com.bloom.source.lib.meta.LEShortColumn;
import com.bloom.source.lib.meta.LongColumn;
import com.bloom.source.lib.meta.StringColumn;
import com.bloom.source.lib.type.columntype;
import com.bloom.source.lib.type.operationtype;
import com.google.protobuf.ByteString;
import com.bloom.common.errors.Error;
import com.webaction.source.cdc.gpb.GPBCommon;
import com.webaction.source.cdc.gpb.GPBCommon._Token;
import com.webaction.source.cdc.gpb.GPBCommon._Token.Builder;
import com.webaction.source.cdc.gpb.GPBRecord;
import com.webaction.source.cdc.gpb.GPBRecord._Column;
import com.webaction.source.cdc.gpb.GPBRecord._DataField;
import com.webaction.source.cdc.gpb.GPBRecord._DataRecord;
import com.webaction.source.cdc.gpb.GPBRecord._DataRecord.OperationType;
import com.webaction.source.cdc.gpb.GPBRecord._MetadataRecord;
import com.webaction.source.cdc.gpb.GPBRecord._Record;
import com.webaction.source.cdc.gpb.GPBRecord._Record.RecordType;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
 
 
 
 /*
  * CDC 数据记录中包括很多有关表、行、列数据的详细信息.
  * 如时间戳、位置、事务ID、表ID、操作类型、裸数据、列簇等.
  */
 
 
 public class CDCDataRecord
   implements IDataRecord
 {
   GPBRecord._DataRecord gpbDataRecord = null;
   List<Integer> colTypes = null;
   int length; int columnCount; int rowOffset = 0;
   long timeStamp;
   String position;
   String transactionId;
   String tableId;
   GPBRecord._DataRecord.OperationType operationType;
   static byte[] intvalbuf; static byte[] longvalbuf; ByteBuffer recordData = null;
   byte[] nullColumns;
   byte[] rowData;
   int[] columnOffset; int[] columnLength; Column[] column = null;
   private SimpleDateFormat ft = new SimpleDateFormat("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
   
 
   CDCRecord cdcRecord = null;
   GPBRecord._DataRecord.Builder gpbDataRecordBuilder = null;
   List<GPBRecord._Column> columnList = new ArrayList();
   
 
 
 
   public CDCDataRecord(CDCRecord record)
   {
     intvalbuf = new byte[4];
     longvalbuf = new byte[8];
     
     this.cdcRecord = record;
     
 
 
     this.gpbDataRecordBuilder = GPBRecord._DataRecord.newBuilder();
   }
   
 
 
 
 
   public CDCDataRecord()
   {
     this.gpbDataRecordBuilder = GPBRecord._DataRecord.newBuilder();
     this.gpbDataRecord = null;
   }
   
 
 
 
 
   public void setGPBDataRecord(GPBRecord._DataRecord gpbDataRecord)
   {
     this.gpbDataRecord = gpbDataRecord;
   }
   
 
 
 
 
   public GPBRecord._DataRecord getGPBDataRecord()
   {
     return this.gpbDataRecord;
   }
   
 
 
   // 定义列类型
 
   public void setColumnTypes(List<Integer> colTypes)
   {
     this.colTypes = colTypes;
   }
   
 
 
 
 
   public List<Integer> getColumnTypes()
   {
     return this.colTypes;
   }
   
 
 
 
 
   public DateTime getTimeStamp()
   {
     this.timeStamp = getGPBDataRecord().getTimestamp();
     return new DateTime(this.timeStamp);
   }
   
 
 
 
 
 
   public int getGPBDataRecordLength()
   {
     return 0;
   }
   
 
 
 
   /**
    * @deprecated
    */
   public int getColumnCountBefore()
   {
     return getBIColumnCount();
   }
   
   public int getBIColumnCount() {
     this.columnCount = getGPBDataRecord().getBiDataFieldCount();
     return this.columnCount;
   }
   
 
 
 
 
   public byte[] getPosition()
   {
     return getGPBDataRecord().getPosition().toByteArray();
   }
   
 
 
 
 
   public String getTransactionId()
   {
     this.transactionId = getGPBDataRecord().getTransactionId().toStringUtf8();
     return this.transactionId;
   }
   
 
 
   // 获得数据记录的元数据
 
   public String getMetaRecordID()
   {
     this.tableId = getGPBDataRecord().getMetarecordId();
     return this.tableId;
   }
   
   // 设置操作类型
 
   public void setOperationType()
   {
     this.operationType = getGPBDataRecord().getOperationtype();
   }
   
 
 
 
 
 
   public operationtype getOperationType()
   {
     operationtype opType = operationtype.WA_INSERT;
     
     switch (getGPBDataRecord().getOperationtype())
     {
     case INSERT: 
       opType = operationtype.WA_INSERT;
       break;
     case UPDATE: 
       opType = operationtype.WA_UPDATE;
       break;
     case DELETE: 
       opType = operationtype.WA_DELETE;
     }
     
     
     return opType;
   }
   
 
 
 
 
 
   public String getOperationName()
   {
     String opType = "INSERT";
     
     switch (getGPBDataRecord().getOperationtype())
     {
     case INSERT: 
       opType = "INSERT";
       break;
     case UPDATE: 
       opType = "UPDATE";
       break;
     case DELETE: 
       opType = "DELETE";
     }
     
     
     return opType;
   }
   
 
   public int[] getColumnLength()
   {
     return this.columnLength;
   }
   
 
   public int getAIColumnCount()
   {
     return this.gpbDataRecord.getAiDataFieldCount();
   }
   
   // 回调后操作-获得列值
   public Object getColumnValueAfter(int index)
     throws CDCException
   {
     GPBRecord._Column gpbColumn = null;
     GPBRecord._DataField dataField = this.gpbDataRecord.getAiDataField(index);
     // 获得列索引
     int colindex = getAIDataFieldIndex(index);
     // 获得MetaRecordID
     String metaDataKey = getMetaRecordID();
     // 查找元记录
     GPBRecord._MetadataRecord metaDataRecord = this.cdcRecord.lookupMetaRecord(metaDataKey);
     
     if (metaDataRecord != null) {
       gpbColumn = metaDataRecord.getColumn(colindex);
     } else {
       throw new CDCException(Error.INVALID_METARECORD_ID, "Meta Record ID : " + metaDataKey);
     }
     
     return getColumnValue(dataField, gpbColumn);
   }
   
 
 
 
   // 回调前操作-获得列值
   public Object getColumnValueBefore(int index)
     throws CDCException
   {
     GPBRecord._Column gpbColumn = null;
     
     GPBRecord._DataField dataField = this.gpbDataRecord.getBiDataField(index);
     
     int colindex = getBIDataFieldIndex(index);
     
     String metaDataKey = getMetaRecordID();
      
     GPBRecord._MetadataRecord metaDataRecord = this.cdcRecord.lookupMetaRecord(metaDataKey);
     
     if (metaDataRecord != null) {
       gpbColumn = metaDataRecord.getColumn(colindex);
     } else {
       throw new CDCException(Error.INVALID_METARECORD_ID, "Meta Record ID : " + metaDataKey);
     }
     
     return getColumnValue(dataField, gpbColumn);
   }
   

 
   // 获得列值
   private Object getColumnValue(GPBRecord._DataField datafield, GPBRecord._Column column)
   {
     if (datafield.getNull())
     {
 
 
       return null;
     }
     
     // 获得列类型
 
     switch (column.getType())
     {
     case 2: 
       return Byte.valueOf((byte)datafield.getSint32Value());
     
 
     case 1: 
     case 4: 
       return Short.valueOf((short)datafield.getInt32Value());
     
 
     case 3: 
     case 6: 
       return Integer.valueOf(datafield.getSint32Value());
     
     case 5: 
     case 8: 
       return Long.valueOf(datafield.getSint64Value());
     case 7: 
       return Long.valueOf(datafield.getInt64Value());
     case 9: 
       return Float.valueOf(datafield.getFloatvalue());
     case 10: 
       return Double.valueOf(datafield.getDoublevalue());
     case 11: 
     case 12: 
     case 17: 
     case 18: 
       return datafield.getStringvalue();
     case 13: 
     case 16: 
       return datafield.getBytesvalue().toByteArray();
     case 14: 
       DateTime ts = new DateTime(datafield.getInt64Value());
       return ts;
     
     case 15: 
       long dateInMillis = datafield.getInt32Value();
       LocalDate date = new LocalDate(dateInMillis * 1000L);
       return date;
     case 0: 
       return "";
     }
 
     return "";
   }
   
 
 
   // 根据Index索引来设置列
   public void setColumn(int index)
   {
     switch (((Integer)this.colTypes.get(index)).intValue()) {
     case 5:  this.column[index] = new LEIntegerColumn();
       break;
     case 11:  this.column[index] = new StringColumn();
       break;
     case 9:  this.column[index] = new LEFloatColumn();
       break;
     case 3:  this.column[index] = new LEShortColumn();
       break;
     case 7:  this.column[index] = new LELongColumn();
       break;
     case 14:  this.column[index] = new LEDatetimeColumn();
       break;
     case 15:  this.column[index] = new LEDateColumn();
       break;
     case 10:  this.column[index] = new LEDoubleColumn();
       break;
     }
     
   }
 
 
   public Column getColumn(int index)
   {
     return this.column[index];
   }
   
   // 设置列的长度
   public void setColumnLength(int columnIndex, int columnLength)
   {
     this.columnLength[columnIndex] = columnLength;
   }
   
 
 
   public void printOperationType()
   {
     setOperationType();
     System.out.println("Operation Type is : " + getOperationName());
   }
   
 
   // 打印出DataRecord的日志信息
 
   public void dump()
   {
     System.out.println("Printing DataRecord");
     System.out.println("<-------------------->");
     
     System.out.println("Timestamp value is : " + getTimeStamp().toString());
     System.out.println("Position value is : " + getPosition());
     System.out.println("Transaction ID is : " + getTransactionId());
     System.out.println("Table ID is : " + getMetaRecordID());
     
     printOperationType();
     
     for (int i = 0; i < this.gpbDataRecord.getAiDataFieldCount(); i++)
     {
 
       System.out.println(this.gpbDataRecord.getAiDataField(i));
     }
   }
   // 设置时间戳
   public void setTimestamp(long timeStamp)
     throws TransactionManagerException
   {
     this.gpbDataRecordBuilder.setTimestamp(timeStamp);
   }
   // 设置LSN值
   public void setLSN(byte[] LSN) throws TransactionManagerException
   {
     ByteString posString = ByteString.copyFrom(LSN);
     this.gpbDataRecordBuilder.setPosition(posString);
   }
   
   public void setTransactionId(byte[] transID)
     throws TransactionManagerException
   {
     ByteString txnIDString = ByteString.copyFrom(transID);
     this.gpbDataRecordBuilder.setTransactionId(txnIDString);
   }
   
   public void setMetaRecordID(String metaRecordID) throws TransactionManagerException
   {
     this.gpbDataRecordBuilder.setMetarecordId(metaRecordID);
   }
   
   public void setOperationType(operationtype operationType)
     throws TransactionManagerException
   {
     if (operationType == operationtype.WA_INSERT) {
       this.gpbDataRecordBuilder.setOperationtype(GPBRecord._DataRecord.OperationType.INSERT);
     } else if (operationType == operationtype.WA_UPDATE) {
       this.gpbDataRecordBuilder.setOperationtype(GPBRecord._DataRecord.OperationType.UPDATE);
     } else if (operationType == operationtype.WA_DELETE) {
       this.gpbDataRecordBuilder.setOperationtype(GPBRecord._DataRecord.OperationType.DELETE);
     }
   }
   
   // 设置列计数
   public void setColumnCount(int columnCount)
     throws TransactionManagerException
   {}
   
 
   // 回调前-设置列值
   public void setColumnValueBefore(int columnIndex, byte[] value, int size, columntype colType)
     throws TransactionManagerException
   {
     GPBRecord._DataField.Builder dataFieldBuilder = getBeforeImageDataField(columnIndex);
     
 
     switch (colType.getType())
     {
 
 
     case 1: 
     case 2: 
       byte byteVal = ByteColumn.getByteValue(value, 0, 8);
       dataFieldBuilder.setInt32Value(byteVal);
       break;
     
     case 3: 
       short shortVal = (short)IntegerColumn.getIntValue(value, 0, 16);
       dataFieldBuilder.setInt32Value(shortVal);
       break;
     
     case 5: 
       int intVal = IntegerColumn.getIntValue(value, 0, 32);
       dataFieldBuilder.setInt32Value(intVal);
       break;
     
     case 6: 
       dataFieldBuilder.setSint32Value(IntegerColumn.getIntValue(value, 0, 32));
       break;
     
     case 7: 
       dataFieldBuilder.setInt64Value(LongColumn.getLongValue(value, 0, 64));
       break;
     
     case 8: 
       dataFieldBuilder.setSint64Value(LongColumn.getLongValue(value, 0, 64));
       break;
     
     case 9: 
       float floatVal = FloatColumn.getFloatValue(value, 0, 32);
       dataFieldBuilder.setFloatvalue(floatVal);
       break;
     
     case 10: 
       double doubleVal = DoubleColumn.getDoubleValue(value, 0, 64).doubleValue();
       dataFieldBuilder.setDoublevalue(doubleVal);
       break;
     
     case 11: 
     case 12: 
     case 17: 
     case 18: 
       dataFieldBuilder.setStringvalue(value.toString());
       break;
     case 13: 
     case 16: 
       dataFieldBuilder.setBytesvalue(ByteString.copyFrom(value));
     
 
 
     case 14: 
       long longVal = LongColumn.getLongValue(value, 0, 64);
       dataFieldBuilder.setInt64Value(longVal);
       break;
     
 
 
 
     case 15: 
       int dateVal = IntegerColumn.getIntValue(value, 0, 32);
       dataFieldBuilder.setInt32Value(dateVal);
       break;
     
     case 0: 
       dataFieldBuilder.setStringvalue("");
     }
     
   }
   
 
 
 
   public void setColumnValueAsStringBefore(int columnIndex, String value)
     throws TransactionManagerException
   {
     GPBRecord._DataField.Builder dataFieldBuilder = getBeforeImageDataField(columnIndex);
     
     dataFieldBuilder.setStringvalue(value);
   }
   
 
 
 
 
   public void setColumnValueAsNullBefore(int columnIndex)
     throws TransactionManagerException
   {
     GPBRecord._DataField.Builder dataFieldBuilder = getBeforeImageDataField(columnIndex);
     
     dataFieldBuilder.setNull(true);
   }
   
 
 
 
 
   public void setColumnValueAfter(int columnIndex, Object value, columntype colType)
     throws TransactionManagerException
   {
     GPBRecord._DataField.Builder dataFieldBuilder = getAfterImageDataField(columnIndex);
     
     setColumnValue(dataFieldBuilder, value, colType);
   }
   
 
 
   public void setColumnValueBefore(int columnIndex, Object value, columntype colType)
     throws TransactionManagerException
   {
     GPBRecord._DataField.Builder dataFieldBuilder = getBeforeImageDataField(columnIndex);
     
     setColumnValue(dataFieldBuilder, value, colType);
   }
   
 
   private void setColumnValue(GPBRecord._DataField.Builder dataFieldBuilder, Object value, columntype colType)
   {
     switch (colType.getType())
     {
 
 
     case 1: 
     case 2: 
       byte byteVal = ((Byte)value).byteValue();
       dataFieldBuilder.setInt32Value(byteVal);
       break;
     
     case 3: 
       short shortVal = ((Short)value).shortValue();
       dataFieldBuilder.setInt32Value(shortVal);
       break;
     
     case 5: 
       dataFieldBuilder.setInt32Value(((Integer)value).intValue());
       break;
     
     case 6: 
       dataFieldBuilder.setSint32Value(((Integer)value).intValue());
       break;
     
     case 7: 
       long longVal = ((Long)value).longValue();
       dataFieldBuilder.setInt64Value(longVal);
       break;
     
     case 8: 
       dataFieldBuilder.setSint64Value(((Long)value).longValue());
       break;
     
     case 9: 
       float floatVal = ((Float)value).floatValue();
       dataFieldBuilder.setFloatvalue(floatVal);
       break;
     
     case 10: 
       double doubleVal = ((Double)value).doubleValue();
       dataFieldBuilder.setDoublevalue(doubleVal);
       break;
     
     case 11: 
     case 12: 
     case 17: 
     case 18: 
       dataFieldBuilder.setStringvalue(value.toString());
       break;
     case 13: 
     case 16: 
       dataFieldBuilder.setBytesvalue(ByteString.copyFromUtf8(value.toString()));
       break;
     
 
 
     case 14: 
       dataFieldBuilder.setInt64Value(((Long)value).longValue());
       break;
     
 
 
 
     case 15: 
       int dateVal = ((Integer)value).intValue();
       dataFieldBuilder.setInt32Value(dateVal);
       break;
     
     case 0: 
       dataFieldBuilder.setStringvalue("");
     }
     
   }
   
 
 
 
   public void setColumnValueAfter(int columnIndex, byte[] value, int size, columntype colType)
     throws TransactionManagerException
   {
     GPBRecord._DataField.Builder dataFieldBuilder = getAfterImageDataField(columnIndex);
     
     switch (colType.getType())
     {
 
 
     case 1: 
     case 2: 
       byte byteVal = ByteColumn.getByteValue(value, 0, 8);
       dataFieldBuilder.setInt32Value(byteVal);
       break;
     
     case 3: 
       short shortVal = (short)IntegerColumn.getIntValue(value, 0, 16);
       dataFieldBuilder.setInt32Value(shortVal);
       break;
     
     case 5: 
       int intVal = IntegerColumn.getIntValue(value, 0, 32);
       dataFieldBuilder.setInt32Value(intVal);
       break;
     
     case 6: 
       dataFieldBuilder.setSint32Value(IntegerColumn.getIntValue(value, 0, 32));
       break;
     
     case 7: 
       long longVal = LongColumn.getLongValue(value, 0, 64);
       dataFieldBuilder.setInt64Value(longVal);
       break;
     
     case 8: 
       dataFieldBuilder.setSint64Value(LongColumn.getLongValue(value, 0, 64));
       break;
     
     case 9: 
       float floatVal = FloatColumn.getFloatValue(value, 0, 32);
       dataFieldBuilder.setFloatvalue(floatVal);
       break;
     
     case 10: 
       double doubleVal = DoubleColumn.getDoubleValue(value, 0, 64).doubleValue();
       dataFieldBuilder.setDoublevalue(doubleVal);
       break;
     
     case 11: 
     case 12: 
     case 17: 
     case 18: 
       dataFieldBuilder.setStringvalue(value.toString());
       break;
     case 13: 
     case 16: 
       dataFieldBuilder.setBytesvalue(ByteString.copyFrom(value));
       break;
     
 
 
     case 14: 
       dataFieldBuilder.setInt64Value(LongColumn.getLongValue(value, 0, 64));
       break;
     
 
 
 
     case 15: 
       int dateVal = IntegerColumn.getIntValue(value, 0, 32);
       dataFieldBuilder.setInt32Value(dateVal);
       break;
     
     case 0: 
       dataFieldBuilder.setStringvalue("");
     }
     
   }
   
 
 
   public void setColumnValueAsStringAfter(int columnIndex, String value)
     throws TransactionManagerException
   {
     GPBRecord._DataField.Builder dataFieldBuilder = getAfterImageDataField(columnIndex);
     
     dataFieldBuilder.setStringvalue(value);
   }
   
 
 
 
   public void setColumnValueAsNullAfter(int columnIndex)
     throws TransactionManagerException
   {
     GPBRecord._DataField.Builder dataFieldBuilder = getAfterImageDataField(columnIndex);
     
     dataFieldBuilder.setNull(true);
   }
   
 
 
 
 
   private GPBRecord._DataField.Builder getBeforeImageDataField(int index)
   {
     GPBRecord._DataField.Builder dataFieldBuilder = null;
     
     dataFieldBuilder = this.gpbDataRecordBuilder.addBiDataFieldBuilder();
     dataFieldBuilder.setIndex(index);
     
     return dataFieldBuilder;
   }
   
 
 
 
   private GPBRecord._DataField.Builder getAfterImageDataField(int index)
   {
     GPBRecord._DataField.Builder dataFieldBuilder = null;
     
     dataFieldBuilder = this.gpbDataRecordBuilder.addAiDataFieldBuilder();
     dataFieldBuilder.setIndex(index);
     
     return dataFieldBuilder;
   }
   
 
 
   public void Process()
   {
     this.gpbDataRecord = this.gpbDataRecordBuilder.build();
 
     clear();
   }
   
 
 
 
   public void clear()
   {
     this.gpbDataRecordBuilder.clear();
   }
   
 
   public byte[] toBytes()
   {
     GPBRecord._Record.Builder gpbRecordBuilder = GPBRecord._Record.newBuilder();
     
     gpbRecordBuilder.setRecordtype(GPBRecord._Record.RecordType.DATA);
     
     gpbRecordBuilder.setDatarecord(this.gpbDataRecordBuilder);
     
     GPBRecord._Record gpbRecord = gpbRecordBuilder.build();
     
 
 
 
 
     return gpbRecord.toByteArray();
   }
   
 
 
 
   public void setColumnValueBefore(int columnIndex, byte[] value, int size)
     throws TransactionManagerException
   {}
   
 
 
 
   public void setColumnValueAfter(int columnIndex, byte[] value, int size)
     throws TransactionManagerException
   {}
   
 
   // 指定元数据记录
 
   public void assignMetadataRecord(GPBRecord._MetadataRecord tempGPBMetaDataRecord)
   {
     if (tempGPBMetaDataRecord.getColumnList().size() != this.columnList.size())
     {
       for (int index = 0; index < tempGPBMetaDataRecord.getColumnList().size(); index++) {
         GPBRecord._Column gpbColumn = tempGPBMetaDataRecord.getColumn(index);
         this.columnList.add(gpbColumn);
       }
     }
   }
   
   public int getAIDataFieldIndex(int index) {
     return this.gpbDataRecord.getAiDataField(index).getIndex();
   }
   
   public int getBIDataFieldIndex(int index) {
     return this.gpbDataRecord.getBiDataField(index).getIndex();
   }
   
 
 
 
   public int getTotalColumnCount()
     throws CDCException
   {
     int colCount = 0;
     String metaDataKey = getMetaRecordID();
     GPBRecord._MetadataRecord metaDataRecord = this.cdcRecord.lookupMetaRecord(metaDataKey);
     
     if (metaDataRecord != null) {
       colCount = metaDataRecord.getColumnCount();
     } else {
       throw new CDCException(Error.INVALID_METARECORD_ID, "Meta Record ID : " + metaDataKey);
     }
     return colCount;
   }
   
   public String getParameterValue(String name)
   {
     String value = null;
     List<GPBCommon._Token> tokenList = this.gpbDataRecord.getTokensList();
    
     for (int i = 0; i < this.gpbDataRecord.getTokensCount(); i++) {
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
     GPBCommon._Token.Builder gpbTokenBuilder = this.gpbDataRecordBuilder.addTokensBuilder();
     
     gpbTokenBuilder.setName(name);
     gpbTokenBuilder.setValue(value);
   }
 }

