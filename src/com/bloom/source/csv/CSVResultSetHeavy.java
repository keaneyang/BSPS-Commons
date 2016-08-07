 package com.bloom.source.csv;
 
 import com.bloom.recovery.CheckpointDetail;
	import com.bloom.source.lib.constant.Constant;
	import com.bloom.source.lib.constant.Constant.status;
	import com.bloom.source.lib.prop.Property;
	import com.bloom.source.lib.reader.Reader;
	import com.bloom.source.lib.reader.ReaderBase;
	import com.bloom.source.lib.rs.CharResultSet;
	import com.bloom.source.nvp.NVPProperty;
	import com.bloom.source.nvp.NameValueParser;
	import com.bloom.common.constants.Constant.recordstatus;
 import com.bloom.common.errors.Error;
 import com.bloom.common.exc.AdapterException;
 import com.bloom.source.sm.QuoteSet;
 import com.bloom.source.sm.SMProperty;
 import com.bloom.source.sm.StateMachine;
 import java.io.IOException;
 import java.nio.CharBuffer;
 import java.util.LinkedList;
 import java.util.List;
 import java.util.Map;
 import java.util.Observable;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern;
 import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class CSVResultSetHeavy
   extends CharResultSet
 {
   Logger logger = Logger.getLogger(CSVResultSetHeavy.class);
   
   CSVResultSetMetaData resultSetMetadata = null;
   
   StateMachine stateMachine;
   
   int rowEndOffset = 0;
   int firstColumnEndPos = 0;
   public long rowStartOffset = 0L;
   int[][] stateDecider;
   recordstatus rstatus;
   CSVProperty csvProp;
   List<String> matchedString = null;
   int columnDelimitTill = -1;
   long fileOffset = 0L;
   int lineNumber = 1; int lineOffset = 0;
   CharBuffer buffer;
   CharBuffer leftOverBuffer;
   boolean ifFileChangedFetchMetadata = false;
   
 
 
 
 
 
 
 
 
   public CSVResultSetHeavy(Reader reader, CSVProperty csvProp)
     throws IOException, InterruptedException, AdapterException
   {
     super(reader, csvProp);
     Init();
     
     this.leftOverBuffer = CharBuffer.allocate(reader().blockSize());
     this.buffer = CharBuffer.allocate(reader().blockSize() * 2);
     this.buffer.flip();
     this.leftOverBuffer.flip();
     
     this.matchedString = new LinkedList();
     this.csvProp = csvProp;
     
 
 
 
 
 
     if (csvProp.headerlineno != 0) {
       this.lineOffset = csvProp.headerlineno;
     }
     
 
 
 
     if (csvProp.lineoffset != 0) {
       this.lineOffset = csvProp.lineoffset;
     }
     this.sourceCheckpoint = reader().getCheckpointDetail();
     
     this.recordCheckpoint = this.sourceCheckpoint;
     
 
 
 
 
 
     this.stateMachine = new StateMachine(new SMProperty(csvProp.propMap));
     
     if ((csvProp.getBoolean("header", false)) && (!this.recordCheckpoint.isRecovery())) {
       this.resultSetMetadata = new CSVResultSetMetaData(this, csvProp);
     }
     
 
 
     this.columnDelimitTill = csvProp.columnDelimitTill;
     reader().registerObserver(this);
     
     if (this.sourceCheckpoint.isRecovery()) {
       this.fileOffset = this.sourceCheckpoint.getRecordEndOffset().longValue();
     }
   }
   
 
 
 
 
   public CSVResultSetMetaData getMetaData()
   {
     return this.resultSetMetadata;
   }
   
 
 
 
 
 
 
 
 
   public recordstatus next()
     throws IOException, InterruptedException
   {
     if (this.logger.isDebugEnabled()) this.logger.debug("Record count is " + getRecordCount());
     int rowEnd = 0;
     int colCount = 0;
     Constant.status retStatus = Constant.status.NORMAL;
     int begin = 0;
     int end = 0;
     int i = 0;
     
     int leftOverRowSize = 0;
     int offsetAdjustment = 0;
     int columnLength = -1;
     
 
 
 
     if (this.ifFileChangedFetchMetadata) {
       this.ifFileChangedFetchMetadata = false;
       recordstatus rs = next();
       if (rs == recordstatus.VALID_RECORD) {
         this.resultSetMetadata = new CSVResultSetMetaData(this);
       }
     }
     
 
 
 
     if (this.buffer.position() < this.buffer.limit())
     {
 
 
 
 
       int rowBegin = this.buffer.position();
       
       if (this.logger.isDebugEnabled()) { this.logger.debug("Buffer Postion is :" + this.buffer.position() + "Block Size is :" + reader().blockSize() + "\n");
       }
       
 
 
       if (getColumnCount() != 0) {
         setColumnCount(0);
       }
       if (this.buffer.position() != this.buffer.limit()) {
         colCount++;
         begin = this.firstColumnEndPos = getRowDataFirstColumnEndPos();
         
         String rowstartOffset = Long.toString(this.fileOffset);
         i += rowstartOffset.length() + this.firstColumnEndPos;
         end = i;
         setColumnOffset(colCount, begin);
         setColumnLength(colCount, end - begin);
         begin = i;
         colCount++;
         rowstartOffset.getChars(0, rowstartOffset.length(), this.rowData, this.firstColumnEndPos);
         this.firstColumnEndPos = begin;
       }
       
 
 
 
 
 
 
 
 
 
 
       while ((this.buffer.position() != this.buffer.limit()) || (this.stateMachine.eventQueue.size() != 0))
       {
 
 
 
 
 
 
 
 
 
         if (this.stateMachine.eventQueue.size() != 0) {
           retStatus = (Constant.status)this.stateMachine.eventQueue.remove();
         } else {
           char c = this.buffer.get();
           retStatus = this.stateMachine.process(c);
           if (retStatus == Constant.status.MULTIPLE_STATUS) {
             retStatus = (Constant.status)this.stateMachine.eventQueue.remove();
             offsetAdjustment = 0;
           } else {
             offsetAdjustment = 1;
           }
         }
         
         if (retStatus == Constant.status.END_OF_COLUMN)
         {
 
 
 
 
           if ((this.lineOffset == 0) || (this.lineNumber >= this.lineOffset))
           {
 
 
 
 
 
 
 
             end = i - this.prop.columndelimiterlist.length();
             
 
 
 
 
 
             if ((this.columnDelimitTill == -1) || (this.columnDelimitTill > colCount))
             {
 
 
               setColumnOffset(colCount, begin);
               setColumnLength(colCount, end - begin);
               
               int offset = 0;
               int len = 0;
               QuoteSet quoteSet = this.stateMachine.matchedQuoteSet();
               if ((this.csvProp.trimQuote) && (quoteSet != null)) {
                 String matchedQuote = quoteSet.toString();
                 len = matchedQuote.length();
                 if (len == 1) {
                   if ((this.buffer.array()[(this.buffer.position() - 3 - (end - begin - 1))] == matchedQuote.charAt(0)) && (this.buffer.array()[(this.buffer.position() - 3)] == matchedQuote.charAt(0)))
                   {
                     offset = 1;
                   }
                 } else if ((this.buffer.array()[(this.buffer.position() - 3 - (end - begin - 1))] == matchedQuote.charAt(0)) && (this.buffer.array()[(this.buffer.position() - 3)] == matchedQuote.charAt(1)))
                 {
                   offset = 1;
                 }
               }
               
               columnLength = end - begin + -1 * offset * 2;
               if ((this.csvProp.ignoreEmptyColums != true) || (columnLength != 0)) {
                 setColumnOffset(colCount, begin + offset);
                 setColumnLength(colCount, columnLength);
                 colCount++;
               }
               begin = end + this.prop.columndelimiterlist.length();
             }
             i++;
           }
           
         }
         else if (retStatus == Constant.status.END_OF_ROW)
         {
 
 
 
 
 
           this.lineNumber += 1;
           
           if ((this.lineOffset != 0) && 
             (this.lineNumber <= this.lineOffset)) {
             rowBegin = this.buffer.position();
 
 
 
           }
           else
           {
 
 
             rowEnd = this.buffer.position();
             
             int rowLength = rowEnd - rowBegin;
             
             this.buffer.position(rowBegin);
             this.buffer.get(this.rowData, this.firstColumnEndPos, rowLength);
             
 
 
 
 
 
             end = i - this.prop.rowdelimiterlist.length() + offsetAdjustment;
             
             int offset = 0;
             int len = 0;
             QuoteSet quoteSet = this.stateMachine.matchedQuoteSet();
             String recDel = this.stateMachine.matchedRowDelimiter();
             
             if ((this.csvProp.trimQuote) && (quoteSet != null)) {
               String matchedQuote = quoteSet.toString();
               len = matchedQuote.length();
               if (len == 1) {
                 if ((this.buffer.array()[(this.buffer.position() - 2 - (end - begin - 1))] == matchedQuote.charAt(0)) && (this.buffer.array()[(this.buffer.position() - (recDel.length() + 1))] == matchedQuote.charAt(0)))
                 {
                   offset = 1;
                 }
               } else if ((this.buffer.array()[(this.buffer.position() - 2 - (end - begin - 1))] == matchedQuote.charAt(0)) && (this.buffer.array()[(this.buffer.position() - (recDel.length() + 1))] == matchedQuote.charAt(1)))
               {
                 offset = 1;
               }
             }
             
 
             columnLength = end - begin + -1 * offset * (recDel.length() + 1);
             if ((this.csvProp.ignoreEmptyColums != true) || (columnLength != 0)) {
               setColumnOffset(colCount, begin + offset);
               setColumnLength(colCount, columnLength);
               
               colCount++;
             }
             setColumnCount(colCount);
             
             setRecordCount(getRecordCount() + 1);
             
 
 
 
 
 
 
 
 
             if (this.recordCheckpoint != null) {
               this.recordCheckpoint.setRecordBeginOffset(this.fileOffset);
               this.recordCheckpoint.setRecordLength(rowLength);
             }
             
             this.rowEndOffset = (this.firstColumnEndPos + rowLength);
             this.rowStartOffset += this.rowEndOffset;
             this.fileOffset += rowLength;
             if (this.stateMachine.getCurrentState() == this.stateMachine.getQuotedCharState()) {
               this.rstatus = recordstatus.INVALID_RECORD;
               this.stateMachine.reset();
             } else {
               this.rstatus = recordstatus.VALID_RECORD;
             }
             
 
 
 
             if (this.recordCheckpoint != null)
               this.recordCheckpoint.setRecordEndOffset(this.fileOffset);
             return this.rstatus;
           }
         } else if (retStatus == Constant.status.NORMAL)
         {
 
 
 
 
           if ((this.lineOffset == 0) || (this.lineNumber >= this.lineOffset))
           {
 
 
 
 
             i++;
           }
         }
         else if (retStatus != Constant.status.IN_COMMENT)
         {
 
 
 
 
 
           if (retStatus == Constant.status.END_OF_COMMENT)
           {
             this.lineNumber += 1;
             
 
 
 
 
             rowBegin = this.buffer.position();
 
 
 
 
 
 
 
 
 
 
           }
           else
           {
 
 
 
 
 
 
 
 
 
             this.stateMachine.reset();
             this.rstatus = recordstatus.ERROR_RECORD;
             return this.rstatus;
           }
         }
       }
       
 
 
 
 
 
 
 
 
 
 
 
 
 
 
       leftOverRowSize = this.buffer.limit() - rowBegin;
       
       this.buffer.position(rowBegin);
       if (leftOverRowSize != 0)
       {
 
 
 
         this.leftOverBuffer.clear();
         this.leftOverBuffer.flip();
         if (leftOverRowSize > this.leftOverBuffer.capacity()) {
           this.logger.error("BUFFER_LIMIT_EXCEED_ERROR - Record size is greater than block size");
         }
         
 
         this.buffer.get(this.leftOverBuffer.array(), 0, leftOverRowSize);
         
 
 
 
         this.buffer.clear();
       } else {
         this.buffer.clear();
       }
     }
     
 
 
 
     CharBuffer tmpBuffer = null;
     try {
       tmpBuffer = (CharBuffer)reader().readBlock();
     } catch (AdapterException se) {
       if (se.getType() == Error.END_OF_DATASOURCE)
         return recordstatus.END_OF_DATASOURCE;
       se.printStackTrace();
     }
     
 
 
 
     if (tmpBuffer == null)
     {
       if (leftOverRowSize != 0)
       {
         this.buffer.put(this.leftOverBuffer.array(), 0, leftOverRowSize);
         this.buffer.position(leftOverRowSize);
         
         this.leftOverBuffer.clear();
         leftOverRowSize = 0;
         this.buffer.flip();
       } else {
         this.buffer.clear();
         this.buffer.limit(0);
       }
       this.rstatus = recordstatus.NO_RECORD;
       return this.rstatus;
     }
     
 
 
 
 
     this.buffer.clear();
     
 
 
     if (leftOverRowSize != 0) {
       this.buffer.put(this.leftOverBuffer.array(), 0, leftOverRowSize);
       this.buffer.position(leftOverRowSize);
       leftOverRowSize = 0;
     }
     
     this.buffer.put(tmpBuffer.array(), 0, tmpBuffer.limit());
     this.buffer.flip();
     
 
 
     this.stateMachine.reset();
     return next();
   }
   
 
 
 
 
   public void setFirstColumnData(String name)
   {
     int i = 0;int end = 0;int colCount = 0;int begin = 0;
     String fileName = name;
     if (fileName == null) {
       fileName = reader().name();
     }
     if (fileName != null) {
       i = fileName.length();
       end = i;
       setColumnOffset(colCount, begin);
       setColumnLength(colCount, end - begin);
       begin = i;
       colCount++;
       fileName.getChars(0, fileName.length(), this.rowData, 0);
     }
     setRowDataFirstColumnEndPos(begin);
   }
   
 
 
   public Constant.recordstatus getRecordStatus()
   {
     return this.rstatus;
   }
   
 
 
   public long getFileOffset()
   {
     return this.fileOffset;
   }
   
 
 
 
 
   public long getRowStartOffset()
   {
     return this.rowStartOffset;
   }
   
 
 
 
 
 
 
   public Map<String, String> getColumnValueAsMap(int index)
     throws AdapterException
   {
     NameValueParser nvp = new NameValueParser(new NVPProperty(this.csvProp.getMap()));
     
     String columnValue = getColumnValue(index);
     
 
 
 
 
     if ((columnValue.charAt(0) == this.csvProp.quotecharacter) && (columnValue.charAt(columnValue.length() - 1) == this.csvProp.quotecharacter))
     {
 
 
 
       columnValue = columnValue.substring(1, columnValue.length() - 1);
       
 
       return nvp.convertToMap(columnValue);
     }
     
     return nvp.convertToMap(columnValue);
   }
   
 
 
 
 
 
 
 
 
 
 
 
 
 
   public List<String> applyRegexOnColumnValue(String columnValue, String regex)
   {
     this.matchedString.clear();
     
 
 
 
     Pattern pattern = Pattern.compile(regex, 2);
     
 
 
 
 
     Matcher patternMatcher = pattern.matcher(columnValue);
     
 
 
 
 
     while (patternMatcher.find()) {
       this.matchedString.add(patternMatcher.group());
     }
     
 
 
 
     return this.matchedString;
   }
   
   public void update(Observable o, Object arg)
   {
     switch ((com.bloom.source.lib.constant.Constant.eventType)arg) {
     case ON_OPEN: 
       setRecordCount(0);
       
 
 
 
       this.lineNumber = 1;
       
       if (this.logger.isInfoEnabled()) this.logger.info(((ReaderBase)o).name() + " is opened ");
       setFirstColumnData(((ReaderBase)o).name());
       
       this.rowStartOffset = 0L;
       this.fileOffset = 0L;
       this.sourceCheckpoint.seekPosition(0L);
       
 
 
 
 
       if ((this.csvProp.header) || (this.csvProp.headerlineno != 0)) {
         this.ifFileChangedFetchMetadata = true;
       }
       break;
     case ON_CLOSE: 
       if (this.logger.isInfoEnabled()) this.logger.info("No of records in the file is " + getRecordCount());
       if ((((ReaderBase)o).name() != null) && 
         (this.logger.isInfoEnabled())) this.logger.info(((ReaderBase)o).name() + " is closed\n");
       break;
     }
   }
   
   public void reset() throws AdapterException
   {
     positionToBeginning();
     reader().skipBytes(this.sourceCheckpoint.seekPosition().longValue());
     this.buffer.clear();
     this.buffer.flip();
     this.stateMachine.reset();
     this.rowEndOffset = 0;
     this.rowStartOffset = 0L;
     this.fileOffset = 0L;
   }
 }

