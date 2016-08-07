package com.bloom.source.nvp;

import java.io.File;
import java.io.IOException;
import java.nio.CharBuffer;

import org.apache.log4j.Logger;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.rs.CharResultSet;
import com.bloom.source.nvp.sm.NVPStateMachine;
import com.bloom.source.smlite.QuoteEvent;
import com.bloom.source.smlite.RowBeginEvent;
import com.bloom.source.smlite.TimeStampEvent;
import com.bloom.common.constants.Constant.recordstatus;

public class NVPResultSet
  extends CharResultSet
{
  CharBuffer buffer;
  Logger logger = Logger.getLogger(NVPResultSet.class);
  NVPStateMachine stateMachine;
  int rowEndOffset = 0;
  int firstColumnEndPos = 0;
  public long rowStartOffset = 0L;
  int[][] stateDecider;
  recordstatus rstatus;
  int prevRowEndOffset = 0;
  int columnDelimitTill = -1;
  boolean endOfResultSet;
  int lineNumber = 1;
  
  public NVPResultSet(NVPProperty prop)
    throws IOException, InterruptedException
  {
    super(null, prop);
    
    Init();
    
    this.stateMachine = new NVPStateMachine(prop);
    
    this.buffer = CharBuffer.allocate(1048576);
  }
  
  protected void Init()
  {
    this.columnOffset = new int[this.prop.maxcolumnoffset];
    this.columnLength = new int[this.prop.maxcolumnoffset];
    this.rowData = new char[1048576];
  }
  
  public void setBuffer(String msg)
  {
    this.buffer.put(msg);
    this.buffer.limit(this.buffer.position());
    this.buffer.rewind();
  }
  
  public recordstatus next()
    throws IOException, InterruptedException
  {
    if ((this.logger.isDebugEnabled()) && (this.logger.isDebugEnabled())) {
      this.logger.debug("Record count is " + getRecordCount());
    }
    int rowEnd = 0;
    int colCount = 0;
    Constant.status retStatus = Constant.status.NORMAL;
    int begin = 0;
    int end = 0;
    int i = 0;
    if (this.endOfResultSet) {
      return recordstatus.ERROR_RECORD;
    }
    int rowBegin = this.buffer.position();
    if (getColumnCount() != 0) {
      setColumnCount(0);
    }
    boolean breakLoop = false;
    while (this.endOfResultSet != true) {
      if (breakLoop)
      {
        breakLoop = false;
      }
      else
      {
        if (this.buffer.position() == this.buffer.limit())
        {
          retStatus = Constant.status.END_OF_ROW;
          this.endOfResultSet = true;
        }
        else if (this.stateMachine.eventQueue.size() != 0)
        {
          retStatus = (Constant.status)this.stateMachine.eventQueue.remove();
        }
        else
        {
          char c = this.buffer.get();
          retStatus = this.stateMachine.process(c);
          if (retStatus == Constant.status.MULTIPLE_STATUS) {
            retStatus = (Constant.status)this.stateMachine.eventQueue.remove();
          }
        }
        if (retStatus == Constant.status.END_OF_COLUMN)
        {
          if ((this.prop.lineoffset == 0) || (this.lineNumber >= this.prop.lineoffset))
          {
            end = i;
            end--;
            
            setColumnOffset(colCount, begin);
            setColumnLength(colCount, end - begin);
            
            begin = end + 1;
            
            colCount++;
            i++;
          }
        }
        else
        {
          if (retStatus == Constant.status.END_OF_ROW)
          {
            this.lineNumber += 1;
            
            rowEnd = this.buffer.position();
            
            int delimiterLength = 0;
            if (!this.endOfResultSet) {
              delimiterLength = this.stateMachine.getRowDelimiterProcessingState().getMatchedDelimiterLength();
            }
            int rowLength = rowEnd - rowBegin - delimiterLength;
            
            this.buffer.position(rowBegin);
            this.buffer.get(this.rowData, this.firstColumnEndPos, rowLength);
            this.buffer.position(rowEnd);
            
            end = rowLength;
            setColumnOffset(colCount, begin);
            setColumnLength(colCount, end - begin);
            
            colCount++;
            setColumnCount(colCount);
            
            setRecordCount(getRecordCount() + 1);
            
            this.rowEndOffset = (this.firstColumnEndPos + rowLength);
            this.rowStartOffset += this.rowEndOffset;
            this.rstatus = recordstatus.VALID_RECORD;
            return this.rstatus;
          }
          if (retStatus == Constant.status.NORMAL) {
            i++;
          } else if (retStatus != Constant.status.IN_COMMENT) {
            if (retStatus == Constant.status.END_OF_COMMENT)
            {
              this.lineNumber += 1;
              
              rowBegin = this.buffer.position();
            }
            else
            {
              long rowBeginFileOffset = 0L;
              long rowCurrentFileOffset = 0L;
              
              this.stateMachine.reset();
              this.rstatus = recordstatus.ERROR_RECORD;
              return this.rstatus;
            }
          }
        }
      }
    }
    return recordstatus.ERROR_RECORD;
  }
  
  public void setFirstColumnData(File file)
  {
    int i = 0;int end = 0;int colCount = 0;int begin = 0;
    String fileName = "test";
    if (fileName != null)
    {
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
  
  public recordstatus getRecordStatus()
  {
    return this.rstatus;
  }
  
  public long getRowStartOffset()
  {
    return this.rowStartOffset;
  }
  
  public void handleEvent(Constant.eventType fileEvent, File file)
    throws IOException
  {
    switch (fileEvent)
    {
    case ON_OPEN: 
      setRecordCount(0);
      
      this.lineNumber = 1;
      if (this.logger.isInfoEnabled()) {
        this.logger.info(file.getName() + " is opened ");
      }
      setFirstColumnData(file);
      this.rowStartOffset = 0L;
      break;
    case ON_CLOSE: 
      if (this.logger.isInfoEnabled()) {
        this.logger.info("No of records in the file is " + getRecordCount());
      }
      break;
    }
  }
  
  public void onEvent(QuoteEvent qEvent) {}
  
  public void onEvent(RowBeginEvent event) {}
  
  public void onEvent(TimeStampEvent event) {}
}
