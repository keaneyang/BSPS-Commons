 package com.bloom.source.csv;
 
 import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.rs.CharResultSet;
import com.bloom.source.smlite.StateMachine;
import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import com.bloom.source.sm.SMProperty;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class CSVResultSet
   extends CharResultSet
 {
   CharResultSet resultSet;
   Logger logger = Logger.getLogger(CharResultSet.class);
   List<String> matchedString;
   Map<String, Pattern[]> patternMap;
   
   public CSVResultSet(Reader reader, CSVProperty csvProp)
     throws IOException, InterruptedException, AdapterException
   {
     super(reader, csvProp);
     if (StateMachine.canHandle(new SMProperty(csvProp.propMap))) {
       if (this.logger.isDebugEnabled())
         this.logger.debug("Using lite state machine");
       this.resultSet = new CSVResultSetLite(reader, new SMProperty(csvProp.propMap));
     } else {
       if (this.logger.isDebugEnabled())
         this.logger.debug("Using old state machine");
       this.resultSet = new CSVResultSetHeavy(reader, csvProp);
     }
     
     this.matchedString = new ArrayList();
     this.patternMap = new HashMap();
   }
   
   public CSVResultSetMetaData getMetaData() {
     return (CSVResultSetMetaData)this.resultSet.getMetaData();
   }
   
   public recordstatus next() throws IOException, InterruptedException {
     return this.resultSet.next();
   }
   
   public Map<String, String> getColumnValueAsMap(int index) throws AdapterException {
     return this.resultSet.getColumnValueAsMap(index);
   }
   
   public String getColumnValue(int index) {
     return this.resultSet.getColumnValue(index);
   }
   
   public int getColumnCount() {
     return this.resultSet.getColumnCount();
   }
   
   public int getRecordCount() {
     return this.resultSet.getRecordCount();
   }
   
   public CheckpointDetail getCheckpointDetail() {
     return this.resultSet.getCheckpointDetail();
   }
   
   public Date eventTime() {
     return this.resultSet.eventTime();
   }
   
   public String getErrorMessage() {
     return this.resultSet.getErrorMessage();
   }
   
   public List<String> applyRegexOnColumnValue(String columnValue, String regEx) {
     String leftOut = "";
     this.matchedString.clear();
     Pattern[] patternArray = (Pattern[])this.patternMap.get(regEx);
     if (patternArray == null) {
       ArrayList<Pattern> patternList = new ArrayList();
       
       for (String patternStr : regEx.split("\\|")) {
         if ((patternStr.charAt(patternStr.length() - 1) == '\\') || (patternStr.charAt(patternStr.length() - 1) != ')'))
         {
           leftOut = leftOut + patternStr + '|';
         }
         else {
           patternStr = leftOut + patternStr;
           leftOut = "";
           try {
             Pattern pattern = Pattern.compile(patternStr);
             patternList.add(pattern);
           } catch (Exception exp) {
             throw new RuntimeException("RegEx compilation error (" + exp.getMessage() + ")", new RecordException(RecordException.Type.NO_RECORD));
           }
         }
       }
       patternArray = (Pattern[])patternList.toArray(new Pattern[patternList.size()]);
       this.patternMap.put(regEx, patternArray);
     }
     
     for (int itr = 0; itr < patternArray.length; itr++) {
       Matcher patternMatcher = patternArray[itr].matcher(columnValue);
       if (patternMatcher.find()) {
         this.matchedString.add(patternMatcher.group());
       } else {
         this.matchedString.add(null);
       }
     }
     return this.matchedString;
   }
   
   public void close() throws AdapterException { this.resultSet.close(); }
 }


