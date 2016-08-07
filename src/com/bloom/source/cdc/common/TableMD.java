 package com.bloom.source.cdc.common;
 
 import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.compiler.TypeField;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.source.cdc.CDCMetaDataRecord;
import com.bloom.source.cdc.CDCRecord;
import com.bloom.source.lib.intf.ITransactionManager;
import com.bloom.source.lib.meta.DatabaseColumn;
import com.webaction.source.cdc.gpb.GPBRecord;
import com.webaction.source.cdc.gpb.GPBRecord._Column;
import com.webaction.source.cdc.gpb.GPBRecord._MetadataRecord;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;
 
 public class TableMD
 {
   private static Logger logger = Logger.getLogger(TableMD.class);
   
 
 
 
 
 
 
 
 
 
 
 
   public static Map<String, TypeDefOrName> getTablesMetadataFromJDBCConnection(List<String> tableList, Connection connection, HashMap<String, ArrayList<Integer>> tableKeyColumns)
   {
     Map<String, TypeDefOrName> tableMetadata = new java.util.TreeMap(String.CASE_INSENSITIVE_ORDER);
     DatabaseColumn column = null;
     DatabaseMetaData dmd = null;
     
     try
     {
       dmd = connection.getMetaData();
       column = DatabaseColumn.initializeDataBaseColumnFromProductName(dmd.getDatabaseProductName());
     } catch (Exception e1) {
       logger.error("Problem in retrieving metadata from database. " + e1, e1);
     }
     for (int i = 0; i < tableList.size(); i++) {
       ArrayList<Integer> keyColumnNumberList = new ArrayList();
       String catalogName = null;String schemaName = null;
       String tableName; 
       if (((String)tableList.get(i)).contains(".")) {
         StringTokenizer tokenizer = new StringTokenizer((String)tableList.get(i), ".");
         if (tokenizer.countTokens() == 3) {
           catalogName = tokenizer.nextToken();
         }
         
         if ((column != null) && ((column instanceof com.bloom.source.lib.meta.MySQLColumn))) {
           catalogName = tokenizer.nextToken();
           tableName = tokenizer.nextToken();
         } else {
           schemaName = tokenizer.nextToken();
           tableName = tokenizer.nextToken();
         }
       }
       else {
         tableName = (String)tableList.get(i);
       }
       try
       {
         ResultSet rs = dmd.getColumns(catalogName, schemaName, tableName, null);
         ResultSet pkResultSet = dmd.getPrimaryKeys(catalogName, schemaName, tableName);
         ArrayList<String> keyColumnList = new ArrayList();
         while (pkResultSet.next()) {
           String keyColName = pkResultSet.getString("COLUMN_NAME");
           
           keyColumnList.add(keyColName);
         }
         
         List<TypeField> columnList = new ArrayList();
         
 
         List<String> columnNameList = new ArrayList();
         
         List<String> modifiedColNameList = new ArrayList();
         List<String> ColumnTypeName = new ArrayList();
         while (rs.next()) {
           columnNameList.add(rs.getString("COLUMN_NAME"));
           ColumnTypeName.add(rs.getString("TYPE_NAME"));
         }
         modifiedColNameList = updateDuplicateColumns(columnNameList);
         for (int index = 0; index < columnNameList.size(); index++) {
           TypeName tn = null;
           if (column != null) {
             column.setInternalColumnType((String)ColumnTypeName.get(index));
             tn = AST.CreateType(com.bloom.source.lib.constant.CDCConstant.getCorrespondingClassForCDCType(column.getInternalColumnType().getType()), 0);
           }
           
           String colName = (String)modifiedColNameList.get(index);
           
 
 
           boolean isKey = keyColumnList.contains(columnNameList.get(index));
           columnList.add(AST.TypeField(colName, tn != null ? tn : AST.CreateType("string", 0), isKey));
           
           if (isKey) {
             keyColumnNumberList.add(Integer.valueOf(index));
           }
         }
         
         TypeDefOrName tableDef = new TypeDefOrName((String)tableList.get(i), columnList);
         
         tableMetadata.put(((String)tableList.get(i)).replace('.', '_'), tableDef);
         tableKeyColumns.put(tableList.get(i), keyColumnNumberList);
         
         columnList = null;
         columnNameList = null;
         modifiedColNameList = null;
         ColumnTypeName = null;
         keyColumnList = null;
         keyColumnNumberList = null;
         rs.close();
         pkResultSet.close();
       } catch (SQLException e) {
         logger.error("Failed to create Striim type for the table " + (String)tableList.get(i) + ". " + e, e);
       }
     }
     
     return tableMetadata;
   }
   
 
 
 
 
 
 
 
 
 
   public static Map<String, TypeDefOrName> getTableMetadataFromGPB(ITransactionManager tm, CDCRecord logRecord)
     throws com.bloom.source.lib.exc.TransactionManagerException
   {
     Map<String, TypeDefOrName> tableMetadata = new java.util.TreeMap(String.CASE_INSENSITIVE_ORDER);
     
     String metarecordId = null;
     
     while ((metarecordId = tm.getNextMetadataRecordID()) != null) {
       CDCMetaDataRecord metaRecord = new CDCMetaDataRecord();
       
       boolean result = tm.getMetadataRecord(metarecordId, metaRecord);
       
 
       if (!result) {
         break;
       }
       
 
 
       metaRecord.setMetadataRecordId(metarecordId);
       metaRecord.Process();
       
 
 
       logRecord.updateMetaRecordCache(metarecordId, metaRecord);
       
 
 
       List<TypeField> columnList = new ArrayList();
       
       List<String> columnNameList = new ArrayList(metaRecord.getColumnCount());
       
       for (int i = 0; i < metaRecord.getColumnCount(); i++) {
         columnNameList.add(metaRecord.getGPBMetaDataRecord().getColumn(i).getName().toUpperCase());
       }
       columnNameList = updateDuplicateColumns(columnNameList);
       for (int i = 0; i < metaRecord.getColumnCount(); i++) {
         GPBRecord._Column column = metaRecord.getGPBMetaDataRecord().getColumn(i);
         TypeName tn = AST.CreateType(com.bloom.source.lib.constant.CDCConstant.getCorrespondingClassForCDCType(column.getType()), 0);
         
 
 
         columnList.add(AST.TypeField((String)columnNameList.get(i), tn, column.getIsKey()));
       }
       
       TypeDefOrName tableDef = new TypeDefOrName(null, columnList);
       
       tableMetadata.put(metarecordId.replace('.', '_'), tableDef);
     }
     return tableMetadata;
   }
   
 
 
 
 
 
   public static List<String> updateDuplicateColumns(List<String> columns)
   {
     List<String> columnNameList = new ArrayList();
     for (int i = 0; i < columns.size(); i++)
     {
 
       String colName = com.bloom.runtime.TypeGenerator.getValidJavaIdentifierName(((String)columns.get(i)).toUpperCase());
       
       if (columnNameList.contains(colName))
       {
         if (!colName.equals(((String)columns.get(i)).toUpperCase()))
         {
           String modified = colName + "_" + (i + 1);
           
 
           while (columnNameList.contains(modified)) {
             modified = modified + "_" + (i + 1);
           }
           
 
           columnNameList.add(modified);
         }
         else {
           int index = columnNameList.indexOf(colName);
           String modified = (String)columnNameList.get(index) + "_" + (index + 1);
           
           columnNameList.remove(index);
           
 
           while (columnNameList.contains(modified)) {
             modified = modified + "_" + (index + 1);
           }
           
 
 
           columnNameList.add(index, modified);
           
           columnNameList.add(colName);
         }
       } else {
         columnNameList.add(colName);
       }
     }
     
     if (logger.isInfoEnabled())
       logger.info("Original column list : " + columns.toString() + "\nModified List : " + columnNameList.toString());
     return columnNameList;
   }
 }

