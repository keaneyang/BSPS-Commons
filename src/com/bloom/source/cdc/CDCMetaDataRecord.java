 package com.bloom.source.cdc;
	import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.intf.IMetadataRecord;
import com.bloom.source.lib.type.columntype;
import com.webaction.source.cdc.gpb.GPBRecord;
 
 import com.webaction.source.cdc.gpb.GPBRecord._Column;
import com.webaction.source.cdc.gpb.GPBRecord._Column.Builder;
import com.webaction.source.cdc.gpb.GPBRecord._MetadataRecord;
import java.io.PrintStream;
import java.util.List;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class CDCMetaDataRecord
   implements IMetadataRecord
 {
   GPBRecord._MetadataRecord gpbMetaDataRecord = null;
   String version;
   String dbName;
   String tableName; int columnCount; CDCRecord cdcRecord = null;
   GPBRecord._MetadataRecord.Builder gpbMetaRecordBuilder = null;
   
   public CDCMetaDataRecord(CDCRecord Record)
   {
     this.cdcRecord = Record;
     
 
 
 
     this.gpbMetaRecordBuilder = GPBRecord._MetadataRecord.newBuilder();
   }
   
   public CDCMetaDataRecord() {
     this.gpbMetaRecordBuilder = GPBRecord._MetadataRecord.newBuilder();
   }
   
 
 
 
 
   public void setGPBMetaDataRecord(GPBRecord._MetadataRecord gpbMetaDataRecord)
   {
     this.gpbMetaDataRecord = gpbMetaDataRecord;
   }
   
 
 
 
 
   public GPBRecord._MetadataRecord getGPBMetaDataRecord()
   {
     return this.gpbMetaDataRecord;
   }
   
 
 
 
 
   public String getVersion()
   {
     this.version = getGPBMetaDataRecord().getVersion();
     return this.version;
   }
   
 
 
 
 
   public String getDataBaseName()
   {
     this.dbName = getGPBMetaDataRecord().getDbname();
     return this.dbName;
   }
   
 
 
 
 
   public String getTableName()
   {
     this.tableName = getGPBMetaDataRecord().getTablename();
     return this.tableName;
   }
   
 
 
 
 
   public int getColumnCount()
   {
     this.columnCount = getGPBMetaDataRecord().getColumnCount();
     return this.columnCount;
   }
   
 
 
 
   public void dump()
   {
     int colCount = 0;
     
     System.out.println("Printing MetdataRecord");
     System.out.println("<--------------------->");
     System.out.println("Version no is " + getVersion());
     System.out.println("Table Name is " + getTableName());
     System.out.println("Database Name is " + getDataBaseName());
     
     colCount = getGPBMetaDataRecord().getColumnCount();
     
     for (int index = 0; index < colCount; index++)
     {
       GPBRecord._Column column = getGPBMetaDataRecord().getColumn(index);
       
       System.out.println("Column Index : " + column.getIndex());
       System.out.println("Column Name : " + column.getName());
       System.out.println("Column Type : " + column.getType());
     }
   }
   
 
 
 
   public void setTimestamp(long timeStamp)
     throws TransactionManagerException
   {}
   
 
 
 
   public void setLSN(byte[] LSN)
     throws TransactionManagerException
   {}
   
 
 
   public void setTransactionId(String transID)
     throws TransactionManagerException
   {}
   
 
 
   public void setVersion(String version)
     throws TransactionManagerException
   {
     this.gpbMetaRecordBuilder.setVersion(version);
   }
   
 
   public void setSchemaName(String ownerName)
     throws TransactionManagerException
   {
     this.gpbMetaRecordBuilder.setSchemaname(ownerName);
   }
   
 
   public void setDatabaseName(String databaseName)
     throws TransactionManagerException
   {
     this.gpbMetaRecordBuilder.setDbname(databaseName);
   }
   
   public void setTableName(String tableName) throws TransactionManagerException
   {
     this.gpbMetaRecordBuilder.setTablename(tableName);
   }
   
 
   public void setColumnCount(int columncount)
     throws TransactionManagerException
   {
     this.columnCount = columncount;
   }
   
 
 
 
 
   public void setColumnType(columntype columnType, int index)
     throws TransactionManagerException
   {
     GPBRecord._Column.Builder columnBuilder = getColumn(index);
     
     columnBuilder.setType(columnType.getType());
   }
   
 
 
 
 
   public void setColumnIsNullable(int columnIndex)
     throws TransactionManagerException
   {
     GPBRecord._Column.Builder columnBuilder = getColumn(columnIndex);
     
     columnBuilder.setIsNullable(true);
   }
   
 
 
 
 
   public void setKeyColumn(int columnIndex)
     throws TransactionManagerException
   {
     GPBRecord._Column.Builder columnBuilder = getColumn(columnIndex);
     
     columnBuilder.setIsKey(true);
   }
   
 
 
 
   public void setColumnPrecision(int precisionValue, int columnIndex)
     throws TransactionManagerException
   {
     GPBRecord._Column.Builder columnBuilder = getColumn(columnIndex);
     
     columnBuilder.setPrecision(precisionValue);
   }
   
 
 
 
   public void setColumnScale(int scaleValue, int columnIndex)
     throws TransactionManagerException
   {
     GPBRecord._Column.Builder columnBuilder = getColumn(columnIndex);
     
     columnBuilder.setScale(scaleValue);
   }
   
 
 
 
   public void setColumnName(String columnName, int columnIndex)
     throws TransactionManagerException
   {
     GPBRecord._Column.Builder columnBuilder = getColumn(columnIndex);
     
     columnBuilder.setName(columnName);
   }
   
 
 
 
   public void setColumnSize(int size, int columnIndex)
     throws TransactionManagerException
   {
     GPBRecord._Column.Builder columnBuilder = getColumn(columnIndex);
     
     columnBuilder.setSize(size);
   }
   
 
 
 
   private GPBRecord._Column.Builder getColumn(int index)
   {
     GPBRecord._Column.Builder columnBuilder = null;
     
 
 
 
     List<GPBRecord._Column> colList = this.gpbMetaRecordBuilder.getColumnList();
     
 
 
 
     int size = colList.size();
     
 
 
 
 
     if (index + 1 > size)
     {
 
 
       for (int i = size; i < index + 1; i++)
       {
 
 
         columnBuilder = this.gpbMetaRecordBuilder.addColumnBuilder(i);
         
         columnBuilder.setIndex(i);
 
       }
       
 
     }
     else
     {
       columnBuilder = this.gpbMetaRecordBuilder.getColumnBuilder(index);
     }
     
 
     return columnBuilder;
   }
   
 
 
 
 
 
 
 
 
 
   public void Process()
   {
     this.gpbMetaDataRecord = this.gpbMetaRecordBuilder.build();
     
 
 
 
 
 
 
 
 
 
     clear();
   }
   
 
 
 
 
 
 
   public String getMetarecordId()
   {
     return getGPBMetaDataRecord().getMetadatarecordId();
   }
   
   public void setMetadataRecordId(String key) {
     this.gpbMetaRecordBuilder.setMetadatarecordId(key);
   }
   
   private void clear()
   {
     this.gpbMetaRecordBuilder.clear();
   }
 }

