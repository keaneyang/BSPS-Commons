 package com.bloom.DBCommons;
 
 import com.bloom.source.WizardCommons.wizardSQL;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
 
 
 
 /*
  * 定义一个数据库的通用类, 定义了数据库连接
  */
 
 
 
 public class DatabaseCommon
 {
   Logger logger = Logger.getLogger(DatabaseCommon.class);
   Connection dbConnection;		
   List<String> tables = null;
   
   public DatabaseCommon(Connection dbCon) {
     this.dbConnection = dbCon;
   }
   
 
 
   /*
    *  得到要访问数据库的相关表, 从数据库连接中获得数据库的元数据, 
    *  通过 catalog, table等元数据获得表的结果集
    */
 
   public List getTables(String fulltableName, String dbType)
     throws Exception
   {
     this.tables = new ArrayList();
     String catalog = null;
     String schema = null;
     String table = null;
     
     try
     {
       String[] tablesArray = fulltableName.split(";");
       for (String tbl : tablesArray) {
         String tableName = tbl.trim();
         
         if (tableName.contains(".")) {
           StringTokenizer tokenizer = new StringTokenizer(tableName, ".");
           if (tokenizer.countTokens() > 3)
             throw new IllegalArgumentException("Illegal argument in TABLES property found. Expected argument should contain at most 3 dot separated string. Found '" + tableName + "'");
           if (tokenizer.countTokens() == 3) {
             catalog = tokenizer.nextToken();
           }
           
           schema = tokenizer.nextToken();
           table = tokenizer.nextToken();
         } else {
           table = tableName;
         }
         if (this.logger.isDebugEnabled())
           this.logger.debug("Trying to fetch table metadata for catalog '" + catalog + "' and schema '" + schema + "' and table pattern '" + table + "'");
         DatabaseMetaData md = this.dbConnection.getMetaData();
         
         ResultSet tableResultSet = md.getTables(catalog, schema, table, new String[] { "TABLE" });
         
         while (tableResultSet.next()) {
           String p1 = tableResultSet.getString(1);
           String p2 = tableResultSet.getString(2);
           String p3 = tableResultSet.getString(3);
           
 
           StringBuilder tableFQN = new StringBuilder();
           if (p1 != null)
             tableFQN.append(p1 + ".");
           if (p2 != null)
             tableFQN.append(p2 + ".");
           tableFQN.append(p3);
           this.tables.add(tableFQN.toString());
           if (this.logger.isInfoEnabled()) {
             this.logger.info("Adding table " + tableFQN.toString() + " to the list of tables to be queried");
           }
         }
         if (dbType.equalsIgnoreCase("oracle")) {
           Statement stmt = null;
           String query = "select * from all_mviews";
           stmt = this.dbConnection.createStatement();
           ResultSet s = stmt.executeQuery(query);
           while (s.next()) {
             this.tables.add(s.getString(1));
           }
         }
       }
     }
     catch (Exception e)
     {
       String errorString = " Failure in fetching tables metadata from Database \n Cause : " + e.getCause() + ";" + "Message : " + e.getMessage();
       
 
       Exception exception = new Exception(errorString);
       
       this.logger.error(errorString);
       throw exception;
     }
     
     return this.tables;
   }
   
 
 
 
   /*
    * 从元数据中得到表的各个列信息, 字段名
    * 
    */
 
 
   public Map getTableColumns(String TableNames)
     throws JSONException, SQLException
   {
     JSONArray newJArray = new JSONArray(TableNames);
     LinkedHashMap tablesAndCol = new LinkedHashMap();
     String catalog = null;
     String schema = null;
     String table = null;
     String columnName = "";
     
     if (newJArray != null) {
       int arrLen = newJArray.length();
       
       for (int i = 0; i < arrLen; i++) {
         LinkedHashMap colAndType = new LinkedHashMap();
         ArrayList<String> allFields = new ArrayList();
         String tn = newJArray.getString(i);
         if (tn.contains(".")) {
           StringTokenizer tokenizer = new StringTokenizer(tn, ".");
           if (tokenizer.countTokens() > 3)
             throw new IllegalArgumentException("Illegal argument in TABLES property found. Expected argument should contain at most 3 dot separated string. Found '" + tn + "'");
           if (tokenizer.countTokens() == 3) {
             catalog = tokenizer.nextToken();
           }
           schema = tokenizer.nextToken();
           table = tokenizer.nextToken();
         } else {
           table = tn;
         }
         
         try
         {
           DatabaseMetaData md = this.dbConnection.getMetaData();
           ResultSet colResultSet = md.getColumns(catalog, schema, table, null);
           while (colResultSet.next()) {
             columnName = colResultSet.getString("COLUMN_NAME");
             String dataType = colResultSet.getString("TYPE_NAME");
             colAndType.put(columnName, dataType);
           }
         } catch (SQLException e) {
           throw e;
         }
         tablesAndCol.put(tn, colAndType);
       }
     }
     
     return tablesAndCol;
   }
   
 
 
 
 
   /*
    * 执行数据库相关语句, 检查数据库的对应版本
    */
 
 
   public boolean checkVersion(String versionToCheck, String dbType)
     throws Exception
   {
     PreparedStatement ps = null;
     ResultSet OVersion = null;
     Boolean correctVersion = Boolean.valueOf(false);
     String[] listOfVersions = versionToCheck.split(",");
     
     try
     {
       switch (dbType.toLowerCase()) {
       case "oracle": 
         ps = this.dbConnection.prepareStatement(wizardSQL.OracleVersionCheck);
         break;
       case "mssql": 
         ps = this.dbConnection.prepareStatement(wizardSQL.MSVersionCheck);
       }
       
       OVersion = ps.executeQuery();
       
       if (OVersion.next()) {
         String ver = OVersion.getString(1);
         
         for (int i = 0; i < listOfVersions.length; i++) {
           if (ver.toLowerCase().contains(listOfVersions[i].toLowerCase())) {
             correctVersion = Boolean.valueOf(true);
           }
         }
         
         if (!correctVersion.booleanValue()) {
           if (this.logger.isInfoEnabled()) this.logger.info("version mismatch. Version found: " + ver);
           throw new Exception("Version Mismatch. Version supported: " + ver);
         }
       }
       if (OVersion != null) OVersion.close();
       if (ps != null) ps.close();
     }
     catch (SQLException e) {
       throw e;
     }
     return correctVersion.booleanValue();
   }
 }

