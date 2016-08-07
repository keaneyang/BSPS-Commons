 package com.bloom.source.csv;
 
 import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.intf.SMCallback;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.reader.ReaderBase;
import com.bloom.source.lib.rs.CharResultSet;
import com.bloom.source.nvp.NVPProperty;
import com.bloom.source.nvp.NameValueParser;
import com.bloom.source.smlite.ColumnEvent;
import com.bloom.source.smlite.CommentEvent;
import com.bloom.source.smlite.EndOfBlockEvent;
import com.bloom.source.smlite.EscapeEvent;
import com.bloom.source.smlite.EventFactory;
import com.bloom.source.smlite.QuoteBeginEvent;
import com.bloom.source.smlite.QuoteEndEvent;
import com.bloom.source.smlite.QuoteEvent;
import com.bloom.source.smlite.ResetEvent;
import com.bloom.source.smlite.RowBeginEvent;
import com.bloom.source.smlite.RowEndEvent;
import com.bloom.source.smlite.RowEvent;
import com.bloom.source.smlite.SMEvent;
import com.bloom.source.smlite.SMProperty;
import com.bloom.source.smlite.StateMachine;
import com.bloom.source.smlite.StateMachineBuilder;
import com.bloom.source.smlite.TimeStampEvent;
import com.bloom.common.constants.Constant;
import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Stack;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 public class CSVResultSetLite
   extends CharResultSet
   implements SMCallback
 {
   Logger logger = Logger.getLogger(CSVResultSetLite.class);
   
   StateMachine sm;
   Reader dataSource;
   int colCount;
   boolean gotCompleteRecord;
   boolean inQuote;
   SMProperty smProp;
   CSVProperty csvProp;
   short[] quoteSetEventIgnoreList;
   short[] quoteEventIgnoreList;
   short[] commentEventIgnoreList;
   short[] columnDelimitTillIgnoreList;
   short[] recordBeginIgnoreEventList;
   CSVResultSetMetaData resultSetMetadata;
   char[] recordBuffer;
   boolean hasGotRecordEnd = false;
   boolean ignoreMultipleRecordBegin = false;
   
   String timeStamp;
   
   int beginOfTimeStamp = -1;
   
   int lengthOfTimeStamp;
   
   String dateString;
   
   boolean hasGotTimestamp;
   
   boolean seenRecordBegin;
   
   int lineSkipCount;
   
   RowEvent rEvent;
   
   RowEvent dummyRowEvent;
   
   boolean hasValidRecord;
   
   boolean inEscape;
   
   int escapeOffset;
   
   boolean seenQuote;
   
   boolean inComment;
   
   boolean delimittedColumn;
   
   boolean rollOverFlag;
   
   long recordBeginOffset = 0L;
   long logicalRecordBeginOffset = 0L;
   long trashedDataLen = 0L;
   ArrayList<Integer> quoteBeginPosition;
   ArrayList<Integer> quoteEndPosition;
   ArrayList<Integer> quoteLength;
   String quoteToMatch;
   Stack<SMEvent> quoteStack;
   String dateFormat;
   Date eventTime;
   Map<String, SimpleDateFormat> dateFormaterMap;
   long chkpntBeginOff;
   long recLength;
   long lastRecordMark;
   int pos;
   int lastPos;
   long lastRecordBeginPosition;
   long unwantedDataLen;
   
   public CSVResultSetLite(Reader reader, Property prop) throws IOException, InterruptedException, AdapterException {
     super(reader, prop);
     this.csvProp = new CSVProperty(prop.getMap());
     this.dataSource = reader;
     this.smProp = new SMProperty(prop.getMap());
     init();
     if (this.logger.isTraceEnabled()) {
       this.logger.trace("CSV/DSVParser is initialized with following properties\nHeader - [" + this.csvProp.header + "]\n" + "RowDelimiter - [" + Arrays.toString(this.smProp.rowDelimiterList) + "]\n" + "ColumnDelimiter - [" + Arrays.toString(this.smProp.columnDelimiterList) + "]\n" + "TrimQuote - [" + this.smProp.trimQuote + "]\n" + "QuoteSet - [" + Arrays.toString(this.smProp.quoteSetList) + "]\n" + "ColumnDelimitTill - [" + this.csvProp.columnDelimitTill + "]\n" + "IgnoreEmptyColumn - [" + this.csvProp.ignoreEmptyColums + "]\n" + "CommentCharacter - [" + this.smProp.commentcharacter + "]\n" + "IgnoreRowDelimiterInQuote - [" + this.smProp.ignoreRowDelimiterInQuote + "]\n" + "HeaderLineNumber - [" + this.csvProp.headerlineno + "]\n" + "NoColumnDelimiter - [" + this.csvProp.nocolumndelimiter + "]\n" + "Separator - [" + (String)prop.getMap().get(":") + "]\n" + "LineNumber - [" + this.csvProp.lineoffset + "]\n" + "TrimWhiteSpace - [" + this.csvProp.trimwhitespace + "]");
     }
   }
   
 
 
 
 
 
 
 
 
 
 
 
 
   private void init()
     throws IOException, InterruptedException, AdapterException
   {
     super.Init();
     
     StateMachineBuilder builder = new StateMachineBuilder(this.smProp);
     this.ignoreMultipleRecordBegin = this.smProp.getBoolean("ignoremultiplerecordbegin", true);
     if ((this.ignoreMultipleRecordBegin) && (this.smProp.recordEnd != null) && (this.smProp.recordEnd.length > 0))
       this.hasGotRecordEnd = true;
     this.sm = builder.createStateMachine(this.dataSource, this.smProp);
     this.sm.callback(this);
     this.dummyRowEvent = ((RowEvent)EventFactory.createEvent((short)2));
     this.dummyRowEvent.array(this.sm.buffer());
     
     if (this.smProp.ignoreRowDelimiterInQuote)
     {
 
 
 
 
       this.quoteEventIgnoreList = new short[8];
       this.quoteEventIgnoreList[0] = 1;
       this.quoteEventIgnoreList[1] = 2;
       this.quoteEventIgnoreList[2] = 8;
       this.quoteEventIgnoreList[3] = 10;
       this.quoteEventIgnoreList[4] = 4;
       this.quoteEventIgnoreList[5] = 5;
       this.quoteEventIgnoreList[6] = 6;
       this.quoteEventIgnoreList[7] = 12;
       
       this.quoteSetEventIgnoreList = new short[8];
       this.quoteSetEventIgnoreList[0] = 1;
       this.quoteSetEventIgnoreList[1] = 2;
       this.quoteSetEventIgnoreList[2] = 8;
       this.quoteSetEventIgnoreList[4] = 3;
       this.quoteSetEventIgnoreList[5] = 5;
       this.quoteSetEventIgnoreList[6] = 6;
       this.quoteSetEventIgnoreList[7] = 12;
     } else {
       this.quoteEventIgnoreList = new short[7];
       this.quoteEventIgnoreList[0] = 1;
       this.quoteEventIgnoreList[1] = 8;
       this.quoteEventIgnoreList[2] = 10;
       this.quoteEventIgnoreList[3] = 4;
       this.quoteEventIgnoreList[4] = 5;
       this.quoteEventIgnoreList[5] = 6;
       this.quoteEventIgnoreList[6] = 12;
       
       this.quoteSetEventIgnoreList = new short[7];
       this.quoteSetEventIgnoreList[0] = 1;
       this.quoteSetEventIgnoreList[1] = 8;
       this.quoteSetEventIgnoreList[3] = 3;
       this.quoteSetEventIgnoreList[4] = 5;
       this.quoteSetEventIgnoreList[5] = 6;
       this.quoteSetEventIgnoreList[6] = 12;
     }
     
 
 
 
     this.commentEventIgnoreList = new short[3];
     this.commentEventIgnoreList[0] = 1;
     this.commentEventIgnoreList[1] = 6;
     this.commentEventIgnoreList[2] = 10;
     
 
 
 
 
     this.columnDelimitTillIgnoreList = new short[2];
     this.columnDelimitTillIgnoreList[0] = 1;
     this.columnDelimitTillIgnoreList[1] = 6;
     
 
 
 
 
     this.recordBeginIgnoreEventList = new short[] { 8 };
     
 
     if (this.sourceCheckpoint == null) {
       this.sourceCheckpoint = this.dataSource.getCheckpointDetail();
     }
     this.recordCheckpoint = this.sourceCheckpoint;
     
     reader().registerObserver(this);
     
     this.dateFormaterMap = new HashMap();
     this.quoteBeginPosition = new ArrayList();
     this.quoteEndPosition = new ArrayList();
     this.quoteLength = new ArrayList();
     this.quoteStack = new Stack();
     
     if ((this.smProp.getBoolean("header", false)) && (!this.recordCheckpoint.isRecovery())) {
       this.resultSetMetadata = new CSVResultSetMetaData(this, new CSVProperty(this.prop.propMap));
     } else {
       this.recordBeginOffset = this.recordCheckpoint.getRecordEndOffset().longValue();
     }
   }
   
   private void resetState() {
     this.lastRecordMark = 0L;
     this.logicalRecordBeginOffset = 0L;
     this.columnBeginOffset[0] = 0;
     this.colCount = 0;
     this.inQuote = false;
     this.inComment = false;
     this.dateFormat = null;
     this.dateString = null;
     this.beginOfTimeStamp = -1;
     this.lengthOfTimeStamp = -1;
     this.seenRecordBegin = false;
     this.hasGotTimestamp = false;
     this.delimittedColumn = false;
     this.sm.ignoreEvents(null);
     this.quoteStack.clear();
     this.quoteEndPosition.clear();
     this.quoteBeginPosition.clear();
     this.quoteLength.clear();
   }
   
   public void onEvent(EscapeEvent event) {
     if (this.inEscape) {
       this.inEscape = false;
       this.escapeOffset = -1;
     } else {
       this.inEscape = true;
       this.escapeOffset = event.position();
     }
   }
   
   public void onEvent(ColumnEvent cEvent)
   {
     if ((this.inEscape) && (escapeEvent(cEvent))) {
       return;
     }
     setTimeStamp(cEvent);
     
     if (!this.inQuote) {
       setColumnData(cEvent);
       if ((this.csvProp.columnDelimitTill != -1) && (this.colCount >= this.csvProp.columnDelimitTill)) {
         this.sm.ignoreEvents(this.columnDelimitTillIgnoreList);
         this.delimittedColumn = true;
       }
     }
     else if (this.logger.isDebugEnabled()) {
       this.logger.debug("Invalid column event detected while in Quote");
     }
   }
   
   public void onEvent(CommentEvent event) {
     if (this.logicalRecordBeginOffset + 1L == event.position()) {
       this.sm.ignoreEvents(this.commentEventIgnoreList);
       this.inComment = true;
     }
   }
   
   public void onEvent(RowEvent rowEvent) {
     if ((this.inComment) || ((this.inEscape) && (escapeEvent(rowEvent))) || ((this.prop.lineoffset > 0) && (this.lineSkipCount < this.prop.lineoffset)))
     {
 
       if (this.inComment) {
         this.sm.ignoreEvents(null);
         this.inComment = false;
       } else if (this.prop.lineoffset > 0) {
         if (this.lineSkipCount + 1 == this.prop.lineoffset)
         {
           this.sm.ignoreEvents(null);
           if (this.delimittedColumn)
             this.delimittedColumn = false;
         }
         this.lineSkipCount += 1;
       }
       this.colCount = 0;
       this.logicalRecordBeginOffset = rowEvent.position();
       this.columnBeginOffset[0] = ((int)this.logicalRecordBeginOffset);
       
       this.sm.lastRecordEndPosition(rowEvent.currentPosition - rowEvent.length);
       
       return;
     }
     
     if (this.delimittedColumn) {
       this.sm.ignoreEvents(null);
       this.delimittedColumn = false;
     }
     
     if (rowEvent.position() == rowEvent.rowBegin()) {
       return;
     }
     if ((this.colCount == 0) && (rowEvent.position() - rowEvent.length() == rowEvent.rowBegin()))
     {
       this.logicalRecordBeginOffset = rowEvent.position();
       this.columnBeginOffset[0] = ((int)this.logicalRecordBeginOffset);
       return;
     }
     
     this.seenRecordBegin = false;
     this.rEvent = rowEvent;
     
     setRecordEnd(rowEvent);
   }
   
   protected void setColumnData(SMEvent event) {
     int offsetAdj = 0;
     if (event.removePattern) {
       this.columnOffset[this.colCount] = (event.currentPosition - event.length);
     } else
       this.columnOffset[this.colCount] = event.currentPosition;
     if ((this.seenQuote) && (this.csvProp.trimQuote)) {
       int colBegin = this.columnBeginOffset[this.colCount];
       int beginOffset = 0;
       
       for (int itr = this.quoteBeginPosition.size() - 1; itr >= 0; beginOffset++) {
         if ((((Integer)this.quoteEndPosition.get(itr)).intValue() != this.columnOffset[this.colCount]) || (colBegin + offsetAdj + ((Integer)this.quoteLength.get(beginOffset)).intValue() != ((Integer)this.quoteBeginPosition.get(beginOffset)).intValue()))
           break;
         this.columnBeginOffset[this.colCount] += ((Integer)this.quoteLength.get(itr)).intValue();
         this.columnOffset[this.colCount] -= ((Integer)this.quoteLength.get(itr)).intValue();
         offsetAdj += ((Integer)this.quoteLength.get(beginOffset)).intValue();itr--;
       }
       
 
 
 
 
       this.seenQuote = false;
     }
     if ((!this.csvProp.ignoreEmptyColums) || (this.columnOffset[this.colCount] - this.columnBeginOffset[this.colCount] > 0)) {
       this.columnBeginOffset[(this.colCount + 1)] = (this.columnOffset[this.colCount] + event.length + offsetAdj);
       offsetAdj = 0;
       this.colCount += 1;
     } else {
       this.columnBeginOffset[this.colCount] = event.position();
     }
     this.quoteEndPosition.clear();
     this.quoteBeginPosition.clear();
     this.quoteLength.clear();
     this.quoteStack.clear();
   }
   
 
 
   private void setRecordEnd(SMEvent event)
   {
     this.sm.canFlush(true);
     if (!this.inQuote)
     {
 
 
 
 
       setTimeStamp(event);
       setColumnData(event);
     }
     
     this.gotCompleteRecord = true;
     long recBegin = this.logicalRecordBeginOffset;
     if ((this.logicalRecordBeginOffset > this.lastRecordMark) && 
       (this.logicalRecordBeginOffset > 0L)) {
       this.recordBeginOffset += this.logicalRecordBeginOffset - this.lastRecordMark;
     }
     
     this.recordBeginOffset += this.trashedDataLen;
     
 
 
 
 
 
 
 
 
 
     this.lastRecordMark = event.currentPosition;
     this.unwantedDataLen = 0L;
     if (event.state() == 2) {
       RowEvent rowEvent = (RowEvent)event;
       this.recordCheckpoint.setRecordBeginOffset(this.recordBeginOffset);
       this.recordCheckpoint.setRecordLength(rowEvent.position() - recBegin);
       this.recordBeginOffset += this.recordCheckpoint.getRecordLength().longValue();
       this.recordCheckpoint.setRecordEndOffset(this.recordBeginOffset);
       this.logicalRecordBeginOffset = event.currentPosition;
     } else {
       if (event.state() == 8) {
         this.recLength = (event.position() - event.length - this.logicalRecordBeginOffset);
         this.logicalRecordBeginOffset = (event.currentPosition - event.length);
         this.sm.rewind(event.length);
       } else if (event.state() == 11) {
         this.recLength = (event.position() - this.logicalRecordBeginOffset);
         this.logicalRecordBeginOffset = event.currentPosition;
       }
       else if (event.state() != 7) {
         this.logger.warn("Only expect to have ROW_BEGIN or ROW_END evnet but we got {" + event.state() + "}." + "Please update the logic to handle it");
       }
       
       this.trashedDataLen = 0L;
       this.rEvent = this.dummyRowEvent;
       this.recordCheckpoint.setRecordLength(this.recLength);
       this.recordCheckpoint.setRecordBeginOffset(this.recordBeginOffset);
       this.recordCheckpoint.setRecordEndOffset(this.recordBeginOffset + this.recLength);
       this.recordBeginOffset += this.recLength;
     }
     this.sm.lastRecordEndPosition((int)this.logicalRecordBeginOffset);
     
     if (this.beginOfTimeStamp != -1) {
       this.dateString = new String(this.rEvent.array(), this.beginOfTimeStamp, this.lengthOfTimeStamp);
       this.beginOfTimeStamp = -1;
     }
     
     setColumnCount(this.colCount + 0);
     this.recordCount += 1;
     
     if (!this.inQuote) {
       this.hasValidRecord = true;
     } else {
       this.inQuote = false;
       this.hasValidRecord = false;
     }
     this.seenRecordBegin = false;
     this.sm.ignoreEvents(null);
     this.sm.canBreak(true);
   }
   
   private void setTimeStamp(SMEvent event) {
     if ((event.isDateEvent) && (((!this.hasGotTimestamp) && (event.state() == 9)) || ((!this.seenRecordBegin) && (event.state() != 9))))
     {
 
 
 
       if (event.state() == 9) {
         this.hasGotTimestamp = true;
       }
       this.dateFormat = event.dateFormat;
       this.beginOfTimeStamp = (event.position() - event.length());
       this.lengthOfTimeStamp = event.length();
       this.beginOfTimeStamp += event.prefixLength;
       this.lengthOfTimeStamp -= event.postfixLength;
     }
   }
   
   private boolean escapeEvent(SMEvent event)
   {
     if (this.inEscape) {
       this.inEscape = false;
       if ((this.escapeOffset == event.position() - event.length()) && 
         (this.logger.isDebugEnabled())) {
         this.logger.debug("Escaping quote event");
       }
       return true;
     }
     return false;
   }
   
   public void onEvent(QuoteEvent event) {
     if ((this.inEscape) && (escapeEvent(event)))
       return;
     this.seenQuote = true;
     if (this.inQuote) {
       this.inQuote = false;
       
 
 
 
 
       this.quoteLength.add(Integer.valueOf(event.length()));
       this.quoteEndPosition.add(Integer.valueOf(event.position()));
       
       this.sm.ignoreEvents(null);
       if (this.delimittedColumn)
       {
 
         this.sm.ignoreEvents(this.columnDelimitTillIgnoreList);
       }
     } else {
       this.quoteBeginPosition.add(Integer.valueOf(event.position()));
       this.inQuote = true;
       this.sm.ignoreEvents(this.quoteEventIgnoreList);
     }
   }
   
   public void onEvent(EndOfBlockEvent event) {
     if ((this.colCount > 0) || (this.columnOffset[0] > this.logicalRecordBeginOffset) || (event.position() > this.logicalRecordBeginOffset))
     {
 
       event.length = 0;
       setRecordEnd(event);
     }
     else {
       this.hasValidRecord = false;
       this.sm.canBreak(true);
     }
   }
   
   public void onEvent(RowEndEvent event) {
     if (this.seenRecordBegin) {
       this.seenRecordBegin = false;
       setRecordEnd(event);
     }
   }
   
   public void onEvent(QuoteBeginEvent event) {
     this.quoteBeginPosition.add(Integer.valueOf(event.position()));
     this.quoteStack.push(event);
     this.inQuote = true;
     this.sm.ignoreEvents(this.quoteSetEventIgnoreList, event.relatedEvent);
   }
   
   public void onEvent(QuoteEndEvent event) {
     this.quoteEndPosition.add(Integer.valueOf(event.position()));
     this.quoteLength.add(Integer.valueOf(event.length()));
     SMEvent beginEvent = (SMEvent)this.quoteStack.pop();
     if (this.quoteStack.isEmpty()) {
       this.inQuote = false;
       this.seenQuote = true;
       this.sm.ignoreEvents(null);
     } else {
       beginEvent = (SMEvent)this.quoteStack.peek();
       this.sm.ignoreEvents(this.quoteSetEventIgnoreList, beginEvent.relatedEvent);
     }
   }
   
 
 
 
 
   public void onEvent(RowBeginEvent event)
   {
     if (!this.seenRecordBegin)
     {
       if (this.hasGotRecordEnd)
       {
         this.sm.ignoreEvents(this.recordBeginIgnoreEventList);
       }
       
       setTimeStamp(event);
       this.seenRecordBegin = true;
       this.logicalRecordBeginOffset = (event.currentPosition - event.length);
       if (this.lastRecordMark < this.logicalRecordBeginOffset)
       {
 
 
 
 
 
 
 
 
         this.unwantedDataLen = (this.logicalRecordBeginOffset - this.lastRecordMark);
       }
       this.sm.lastRecordEndPosition((int)this.logicalRecordBeginOffset);
       this.colCount = 0;
       if (event.removePattern) {
         this.columnBeginOffset[this.colCount] = event.position();
       } else
         this.columnBeginOffset[this.colCount] = (event.position() - event.length());
       this.sm.canFlush(false);
     } else {
       setRecordEnd(event);
     }
   }
   
   public void onEvent(TimeStampEvent event)
   {
     setTimeStamp(event);
   }
   
 
   public void onEvent(ResetEvent event)
   {
     this.trashedDataLen += event.currentPosition;
     
 
     this.trashedDataLen += this.unwantedDataLen;
     this.unwantedDataLen = 0L;
     this.rollOverFlag = true;
     this.recordBuffer = event.buffer;
     resetState();
   }
   
   public Constant.recordstatus next() throws IOException, InterruptedException {
     this.colCount = 0;
     this.columnBeginOffset[0] = ((int)this.logicalRecordBeginOffset);
     this.gotCompleteRecord = false;
     
     this.sm.canBreak(false);
     this.sm.canFlush(true);
     this.eventTime = null;
     this.dateString = null;
     this.hasGotTimestamp = false;
     this.rollOverFlag = false;
     this.seenRecordBegin = false;
     this.quoteStack.clear();
     try {
       while (this.gotCompleteRecord != true) {
         this.sm.next();
       }
       if ((this.resultSetMetadata != null) && (this.resultSetMetadata.isValid != true)) {
         this.resultSetMetadata = new CSVResultSetMetaData(this);
         return next();
       }
     } catch (RecordException rExp) {
       this.errorMessage = rExp.errMsg();
       if (rExp.returnStatus() != Constant.recordstatus.NO_RECORD) {
         this.logger.warn("Got RecordException : {" + this.errorMessage + "}");
       }
       return rExp.returnStatus();
     } catch (AdapterException sExp) {
       if (sExp.getErrorMessage() != null) {
         this.errorMessage = (sExp.getErrorMessage() + " data source :{" + reader().name() + "}");
       } else {
         this.errorMessage = ("Got SourceException. Data source :{" + reader().name() + "}");
       }
       if (sExp.getType() == Error.END_OF_DATASOURCE) {
         if (this.logger.isTraceEnabled()) {
           this.logger.trace("Reached end of data source");
         }
         return Constant.recordstatus.END_OF_DATASOURCE;
       }
       
       if (this.isClosed) {
         this.logger.error("next() is called on a closed result-set. Exception : {" + this.errorMessage + "}");
       } else {
         this.logger.warn("Got SourceException :{" + sExp.getMessage() + "}");
       }
       return Constant.recordstatus.ERROR_RECORD;
     } catch (RuntimeException runExp) {
       Throwable cause = runExp.getCause();
       if ((cause != null) && ((cause instanceof RecordException)) && (((RecordException)cause).type() == RecordException.Type.END_OF_DATASOURCE)) {
         if (this.logger.isTraceEnabled()) {
           this.logger.trace("Reached end of data source");
         }
         return Constant.recordstatus.END_OF_DATASOURCE;
       }
       this.logger.warn("Got exception while calling next()", runExp);
     }
     catch (Exception exp) {
       exp.printStackTrace();
       return Constant.recordstatus.NO_RECORD;
     }
     
     if (this.hasValidRecord) {
       return Constant.recordstatus.VALID_RECORD;
     }
     this.logger.warn("Invalid Record is encountered at : [" + getCheckpointDetail().toString() + "]");
     clearInternalState();
     return Constant.recordstatus.INVALID_RECORD;
   }
   
   private void clearInternalState() {
     this.quoteStack.clear();
     this.quoteEndPosition.clear();
     this.quoteBeginPosition.clear();
     this.quoteLength.clear();
   }
   
   public String getColumnValue(int colIdx) {
     String tmp = new String(this.recordBuffer, this.columnBeginOffset[colIdx], this.columnOffset[colIdx] - this.columnBeginOffset[colIdx]);
     if (this.csvProp.trimwhitespace)
       return tmp.trim();
     return tmp;
   }
   
   public CSVResultSetMetaData getMetaData()
   {
     return this.resultSetMetadata;
   }
   
   public Date eventTime() {
     if (this.dateString != null) {
       SimpleDateFormat dateFormater = (SimpleDateFormat)this.dateFormaterMap.get(this.dateFormat);
       if (dateFormater == null) {
         dateFormater = new SimpleDateFormat(this.dateFormat);
         this.dateFormaterMap.put(this.dateFormat, dateFormater);
       }
       try {
         this.eventTime = dateFormater.parse(this.dateString);
       } catch (ParseException e) {
         this.logger.warn("Couldn't convert [" + this.dateString + "] the text using [" + dateFormater.toPattern().toString() + "] Record # : [" + this.recordCount + "]");
         return null;
       }
       return this.eventTime;
     }
     return null;
   }
   
 
 
 
   public void reset()
     throws AdapterException
   {
     boolean invalidateBuffer = false;
     positionToBeginning();
     if (this.recordCheckpoint != null)
       invalidateBuffer = reader().skipBytes(this.recordCheckpoint.seekPosition().longValue()) != 0L;
     this.sm.reset(invalidateBuffer);
     if (invalidateBuffer) {
       this.recordBeginOffset = 0L;
     }
   }
   
 
 
   public void update(Observable o, Object arg)
   {
     switch ((com.bloom.source.lib.constant.Constant.eventType)arg) {
     case ON_OPEN: 
       setRecordCount(0);
       
       if (this.logger.isInfoEnabled()) { this.logger.info(((ReaderBase)o).name() + " is opened ");
       }
       this.sourceCheckpoint.seekPosition(0L);
       this.sourceCheckpoint.setSourceName(this.dataSource.name());
       this.recordBeginOffset = 0L;
       this.lineSkipCount = 0;
       
 
 
 
 
       if ((this.csvProp.header) || (this.csvProp.headerlineno != 0)) {
         if (this.resultSetMetadata != null)
           this.resultSetMetadata.isValid = false;
         if (this.logger.isTraceEnabled())
           this.logger.trace("New file is opened and header will be skipped");
       }
       break;
     case ON_CLOSE: 
       if (this.logger.isInfoEnabled()) this.logger.info("No of records in the file is " + getRecordCount());
       if ((((ReaderBase)o).name() != null) && 
         (this.logger.isInfoEnabled())) this.logger.info(((ReaderBase)o).name() + " is closed\n");
       break;
     }
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
   
   public void close() throws AdapterException {
     this.sm.close();
     super.close();
   }
 }

