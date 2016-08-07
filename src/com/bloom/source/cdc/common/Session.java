 package com.bloom.source.cdc.common;
 
 import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.intf.IPosition;
import com.bloom.source.lib.intf.ISession;
import com.bloom.source.lib.type.positiontype;
import com.bloom.source.lib.type.sessiontype;
import com.bloom.common.errors.Error;
import com.webaction.source.cdc.gpb.GPBCommon;
import com.webaction.source.cdc.gpb.GPBCommon._Session;
import com.webaction.source.cdc.gpb.GPBCommon._Session.Builder;
import com.webaction.source.cdc.gpb.GPBCommon._Session.SessionType;
import com.webaction.source.cdc.gpb.GPBCommon._Table;
import com.webaction.source.cdc.gpb.GPBCommon._Token;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class Session
   implements ISession
 {
   int errorNo = 0;
   Position cdcPosition;
   positiontype positionType = positiontype.WA_POSITION_EOF;
   sessiontype sessionType;
   Map<String, Object> sessionConfig = null;
   GPBCommon._Session gpbSession = null;
   GPBCommon._Session.Builder gpbSessionBuilder = null;
   GPBCommon._Token gpbToken = null;
   GPBCommon._Token.Builder gpbTokenBuilder = null;
   GPBCommon._Table gpbTable = null;
   GPBCommon._Table.Builder gpbTableBuilder = null;
   
 
   Logger logger = Logger.getLogger(Session.class);
   String[] tableList;
   Map<String, String> tableNameMap;
   final String includeColumns = "+"; final String excludeColumns = "-"; final String openParanthesis = "("; final String closeParanthesis = ")";
   
 
 
 
 
 
 
 
 
 
 
   public Session(positiontype positionType, byte[] positionValue, Map<String, Object> prop)
     throws CoreException
   {
     this.sessionConfig = prop;
     String inputSessionType = (String)this.sessionConfig.get("SessionType");
     this.sessionConfig.remove("SessionType");
     
     if (inputSessionType != null) {
       if (inputSessionType.equalsIgnoreCase("METADATA")) {
         this.sessionType = sessiontype.WA_METADATA_SESSION;
       } else {
         this.sessionType = sessiontype.WA_BOTH;
       }
     } else {
       this.sessionType = sessiontype.WA_BOTH;
     }
     this.cdcPosition = new Position(positionType, positionValue);
     
     this.gpbSessionBuilder = GPBCommon._Session.newBuilder();
     
     this.gpbTokenBuilder = GPBCommon._Token.newBuilder();
     
     this.gpbTableBuilder = GPBCommon._Table.newBuilder();
     
 
 
 
 
 
 
     this.tableNameMap = new HashMap();
   }
   
 
 
 
 
   private GPBCommon._Session.SessionType getGPBSessionType()
   {
     GPBCommon._Session.SessionType sessType = null;
     
     switch (this.sessionType) {
     case WA_BOTH: 
       sessType = GPBCommon._Session.SessionType.BOTH;
       break;
     case WA_DATA_SESSION: 
       sessType = GPBCommon._Session.SessionType.DATA_SESSION;
       break;
     case WA_METADATA_SESSION: 
       sessType = GPBCommon._Session.SessionType.METADATA_SESSION;
     }
     
     
 
     return sessType;
   }
   
 
 
 
 
 
 
 
 
   public GPBCommon._Session.Builder getGPBSession()
     throws CDCException
   {
     this.gpbSessionBuilder.setPosition(this.cdcPosition.getGPBPosition());
     
 
 
 
     this.gpbSessionBuilder.setType(getGPBSessionType());
     
 
 
 
 
     try
     {
       String[] excludedTables = getExcludedTableList();
       if (excludedTables != null) {
         for (String excludedTable : excludedTables) {
           this.gpbSessionBuilder.addExcludedtablelist(excludedTable);
         }
       }
     } catch (TransactionManagerException e) {
       CDCException cdcException = new CDCException(Error.FAILED_SESSION_INITIALIZATION, e);
       throw cdcException;
     }
     
 
 
 
     try
     {
       this.tableList = getTableList();
       if (this.tableList != null) {
         for (int i = 0; i < this.tableList.length; i++)
           this.gpbSessionBuilder.addTablelist(getGPBTable(this.tableList[i]));
       }
     } catch (TransactionManagerException e) {
       CDCException cdcException = new CDCException(Error.FAILED_SESSION_INITIALIZATION, e);
       throw cdcException;
     }
     
     this.sessionConfig.remove("Tables");
     
     if (this.sessionConfig != null)
     {
 
 
       Set<String> tokenNames = this.sessionConfig.keySet();
       
 
 
 
       Iterator<String> tokenIterator = tokenNames.iterator();
       
 
 
 
       while (tokenIterator.hasNext())
       {
         String key = (String)tokenIterator.next();
         
         if ((this.sessionConfig.get(key) instanceof String)) {
           setToken(key, (String)this.sessionConfig.get(key));
           
 
 
           this.gpbSessionBuilder.addTokens(buildToken());
         }
       }
     }
     
 
 
     return this.gpbSessionBuilder;
   }
   
 
 
 
 
 
 
 
 
 
 
 
   private GPBCommon._Table getGPBTable(String tableName)
     throws CDCException
   {
     this.gpbTableBuilder.setName(tableName);
     
 
 
 
 
     try
     {
       String[] interestedColumns = getColumns(tableName);
       if (interestedColumns != null) {
         for (String interestedColumn : interestedColumns) {
           this.gpbTableBuilder.addColumnlist(interestedColumn);
         }
       }
       
 
 
 
 
       String[] excludedColumns = getExcludedColumns(tableName);
       if (excludedColumns != null) {
         for (String excludedColumn : excludedColumns) {
           this.gpbTableBuilder.addExcludedcolumnlist(excludedColumn);
         }
       }
     } catch (TransactionManagerException e) {
       CDCException cdcException = new CDCException(Error.FAILED_SESSION_INITIALIZATION, e);
       throw cdcException;
     }
     
 
 
 
     this.gpbTable = this.gpbTableBuilder.build();
     
 
 
 
 
 
     this.gpbTableBuilder.clear();
     
     return this.gpbTable;
   }
   
 
 
 
 
 
 
 
 
   private String getTableName(String tableNameWithColumns)
     throws CoreException
   {
     String tableName = tableNameWithColumns;
     if (tableName.contains("(")) {
       int startIndexOfColumn = tableName.indexOf("(");
       
 
 
       if (startIndexOfColumn == 0) {
         throw new CoreException(Error.INVALID_TABLENAME, " Table name " + tableName + " has only column information of that table and not the actual table name");
       }
       tableName = tableName.substring(0, startIndexOfColumn);
     }
     return tableName;
   }
   
 
 
 
 
 
 
 
   private void setToken(String name, String value)
   {
     this.gpbTokenBuilder.setName(name);
     this.gpbTokenBuilder.setValue(value);
   }
   
 
 
 
 
   private GPBCommon._Token buildToken()
   {
     this.gpbToken = this.gpbTokenBuilder.build();
     return this.gpbToken;
   }
   
   public sessiontype getSessionType()
     throws TransactionManagerException
   {
     return this.sessionType;
   }
   
 
 
 
 
   public void setSessionType(sessiontype type)
   {
     this.sessionType = type;
   }
   
   public IPosition getPosition()
     throws TransactionManagerException
   {
     return this.cdcPosition;
   }
   
   public String getParameterValue(String name) throws TransactionManagerException
   {
     return this.sessionConfig.get(name).toString();
   }
   
   public Map<String, Object> getPropertyMap() throws TransactionManagerException
   {
     return this.sessionConfig;
   }
   
   public String[] getTableList() throws TransactionManagerException
   {
     if ((getPropertyMap().containsKey("Tables")) && (getPropertyMap().get("Tables") != null)) {
       String tablesNameList = (String)getPropertyMap().get("Tables");
       tablesNameList = tablesNameList.replaceAll("\\s+", "");
       String[] tableNamesWithColumns = tablesNameList.split(";");
       String[] tableNames = new String[tableNamesWithColumns.length];
       for (int i = 0; i < tableNamesWithColumns.length; i++)
       {
 
 
 
 
         tableNames[i] = getTableName(tableNamesWithColumns[i]);
         
 
 
 
 
         this.tableNameMap.put(tableNames[i], tableNamesWithColumns[i]);
       }
       return tableNames;
     }
     
     return null;
   }
   
   public String[] getExcludedTableList() throws TransactionManagerException
   {
     if ((getPropertyMap().containsKey("ExcludedTables")) && (getPropertyMap().get("ExcludedTables") != null)) {
       String excludedTableNames = (String)getPropertyMap().get("ExcludedTables");
       return excludedTableNames.split(";");
     }
     
     return null;
   }
   
 
 
 
 
   public String[] getColumns(String table)
     throws TransactionManagerException
   {
     String tableName = (String)this.tableNameMap.get(table);
     if (tableName != null)
     {
       String columnList = getColumnListFromTable(tableName);
       
 
 
 
 
       if (columnList != null) {
         if ((columnList.contains("+")) && (columnList.contains("-"))) {
           CDCException cdcException = new CDCException(Error.INVALID_TABLE_FORMAT);
           throw cdcException;
         }
         
         if (columnList.contains("+"))
         {
           columnList = columnList.replaceAll("\\+", "");
           return extractColumns(columnList);
         }
       }
     }
     return null;
   }
   
 
 
 
 
   public String[] getExcludedColumns(String table)
     throws TransactionManagerException
   {
     String tableName = (String)this.tableNameMap.get(table);
     if (tableName != null)
     {
       String columnList = getColumnListFromTable(tableName);
       
 
 
 
 
       if (columnList != null) {
         if ((columnList.contains("+")) && (columnList.contains("-"))) {
           CDCException cdcException = new CDCException(Error.INVALID_TABLE_FORMAT);
           throw cdcException;
         }
         
         if (columnList.contains("-")) {
           columnList = columnList.replaceAll("-", "");
           return extractColumns(columnList);
         }
       }
     }
     return null;
   }
   
 
 
 
 
 
 
   private String getColumnListFromTable(String tableName)
   {
     if ((tableName.contains("(")) && (tableName.contains(")"))) {
       int beginIndex = tableName.indexOf("(");
       int endIndex = tableName.indexOf(")");
       return tableName.substring(beginIndex + 1, endIndex);
     }
     
     return null;
   }
   
 
 
 
 
 
 
 
   private String[] extractColumns(String columnList)
   {
     String[] extractedColumns = columnList.split(",");
     
 
 
 
     for (int i = 0; i < extractedColumns.length; i++) {
       extractedColumns[i] = extractedColumns[i].trim();
     }
     return extractedColumns;
   }
 }

