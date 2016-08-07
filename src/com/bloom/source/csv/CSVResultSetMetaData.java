 package com.bloom.source.csv;
 
import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.constant.Constant.fieldType;
import com.bloom.source.lib.meta.Column;
import com.bloom.source.lib.meta.StringColumn;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.rs.CharResultSet;
import com.bloom.source.lib.rs.MetaData;
import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class CSVResultSetMetaData
   extends MetaData
 {
   private String nextFileName = null;
   private String prevFileName = null;
   
   CSVProperty prop;
   CharResultSet resultSet;
   Hashtable<Integer, Column> columnMetaData;
   int columnCount = 0;
   public boolean isValid = false;
   static Logger logger = Logger.getLogger(CSVResultSetMetaData.class);
   
 
 
 
 
 
 
 
 
   public CSVResultSetMetaData(CharResultSet waResultSet)
     throws IOException, InterruptedException
   {
     this.columnCount = waResultSet.getColumnCount();
     
     if (this.columnCount > 0) {
       this.isValid = true;
       
 
 
       this.columnMetaData = new Hashtable(this.columnCount);
       
       for (int i = 0; i < this.columnCount; i++) {
         Column column = new StringColumn();
         column.setIndex(i);
         column.setName(waResultSet.getColumnValue(i));
         column.setType(Constant.fieldType.STRING);
         
 
 
 
 
         this.columnMetaData.put(Integer.valueOf(i), column);
       }
     }
   }
   
   public CSVResultSetMetaData(CharResultSet waResultSet, CSVProperty csvprop) throws IOException, InterruptedException, AdapterException
   {
     this.prop = csvprop;
     this.resultSet = waResultSet;
     
 
 
 
 
 
     if (this.resultSet.reader().name() != null) {
       if (logger.isDebugEnabled()) logger.debug("If header isn't present in the property no action is taken on" + getCurrentFileName());
       if ((this.prop.header) || (this.prop.headerlineno != 0))
       {
         try {
           if (csvprop.positionByEOF) {
             this.resultSet.positionToBeginning();
           }
           recordstatus rs = this.resultSet.next();
           if (rs == recordstatus.VALID_RECORD) {
             this.isValid = true;
           }
         } catch (IOException e) {
           logger.error(e);
         }
         
         this.columnCount = this.resultSet.getColumnCount();
         
 
 
 
         this.columnMetaData = new Hashtable(this.columnCount);
         
         for (int i = 0; i < this.columnCount; i++) {
           Column column = new StringColumn();
           column.setIndex(i);
           column.setName(this.resultSet.getColumnValue(i));
           column.setType(Constant.fieldType.STRING);
           
 
 
 
 
           this.columnMetaData.put(Integer.valueOf(i), column);
         }
         
         if (csvprop.positionByEOF) {
           this.resultSet.reset();
         }
       }
     }
   }
   
 
 
 
   public List<String> getInterestedMetadata()
   {
     String metadataRegex = this.prop.metaColumnList;
     
 
 
 
 
     Pattern pattern = Pattern.compile(metadataRegex, 2);
     
     List<String> interestedMetadataList = new LinkedList();
     
     for (int index = 0; index < this.columnCount; index++)
     {
       String value = getColumnName(index);
       
 
 
 
 
       Matcher patternMatcher = pattern.matcher(value);
       
 
 
 
 
       while (patternMatcher.find()) {
         interestedMetadataList.add(patternMatcher.group());
       }
     }
     
 
 
 
     return interestedMetadataList;
   }
   
 
 
 
   public String getCurrentFileName()
   {
     return this.resultSet.reader().name();
   }
   
 
 
   public String getNextFileName()
   {
     return this.nextFileName;
   }
   
 
 
   public String getPrevFileName()
   {
     return this.prevFileName;
   }
   
 
 
   public int getColumnCount()
   {
     return this.columnMetaData.size();
   }
   
 
 
 
   public String getColumnName(int index)
   {
     if (!this.columnMetaData.isEmpty())
     {
       return ((Column)this.columnMetaData.get(Integer.valueOf(index))).getName();
     }
     
     return null;
   }
   
 
 
   public Constant.fieldType getColumnType(int index)
   {
     if (!this.columnMetaData.isEmpty())
     {
       return ((Column)this.columnMetaData.get(Integer.valueOf(index))).getType();
     }
     return Constant.fieldType.STRING;
   }
 }

