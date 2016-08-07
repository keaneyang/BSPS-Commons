package com.bloom.source.lib.rs;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.intf.SMCallback;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.smlite.ColumnEvent;
import com.bloom.source.smlite.CommentEvent;
import com.bloom.source.smlite.EndOfBlockEvent;
import com.bloom.source.smlite.EscapeEvent;
import com.bloom.source.smlite.NVPEvent;
import com.bloom.source.smlite.QuoteBeginEvent;
import com.bloom.source.smlite.QuoteEndEvent;
import com.bloom.source.smlite.QuoteEvent;
import com.bloom.source.smlite.ResetEvent;
import com.bloom.source.smlite.RowBeginEvent;
import com.bloom.source.smlite.RowEndEvent;
import com.bloom.source.smlite.RowEvent;
import com.bloom.source.smlite.SMEvent;
import com.bloom.source.smlite.TimeStampEvent;
import com.bloom.common.constants.Constant;
import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import org.apache.log4j.Logger;

public abstract class ResultSet
  implements Observer, SMCallback
{
  private char[] rowMetaData;
  private int columnCount = 0;
  protected int recordCount = 0;
  protected Property prop;
  boolean matchFound = false;
  int firstColumnEndPos = 0;
  Reader reader;
  protected String errorMessage;
  protected boolean isClosed;
  protected CheckpointDetail sourceCheckpoint;
  protected CheckpointDetail recordCheckpoint;
  protected int[] columnOffset;
  protected int[] columnBeginOffset;
  protected int[] columnLength;
  Logger logger = Logger.getLogger(ResultSet.class);
  
  public ResultSet(Reader reader, Property prop)
    throws IOException, InterruptedException
  {
    this.prop = prop;
    this.reader = reader;
  }
  
  protected void Init()
    throws IOException, InterruptedException
  {
    this.columnOffset = new int[this.prop.maxcolumnoffset];
    this.columnLength = new int[this.prop.maxcolumnoffset];
    this.columnBeginOffset = new int[this.prop.maxcolumnoffset];
    setFirstColumnData(null);
    this.firstColumnEndPos = getRowDataFirstColumnEndPos();
    this.rowMetaData = new char[this.reader.blockSize()];
    this.recordCheckpoint = new CheckpointDetail(this.reader.getCheckpointDetail());
    this.sourceCheckpoint = new CheckpointDetail(this.reader.getCheckpointDetail());
  }
  
  public Reader reader()
  {
    return this.reader;
  }
  
  public int getRowDataFirstColumnEndPos()
  {
    return this.firstColumnEndPos;
  }
  
  public void setRowDataFirstColumnEndPos(int firstColumnEndPos)
  {
    this.firstColumnEndPos = firstColumnEndPos;
  }
  
  public void setFirstColumnData(String fileName) {}
  
  public String getColumnName(int columnIndex)
  {
    return String.valueOf(this.rowMetaData, this.columnOffset[columnIndex], this.columnLength[columnIndex]);
  }
  
  public void close()
    throws AdapterException
  {
    try
    {
      this.isClosed = true;
      this.reader.close();
    }
    catch (IOException exp)
    {
      throw new AdapterException("Problem closing the reader " + this.reader.name(), exp);
    }
  }
  
  public abstract recordstatus next()
    throws IOException, InterruptedException;
  
  public int getColumnCount()
  {
    return this.columnCount;
  }
  
  public int getRecordCount()
  {
    return this.recordCount;
  }
  
  public void setRecordCount(int recordcount)
  {
    this.recordCount = recordcount;
  }
  
  public void setColumnCount(int columncount)
  {
    this.columnCount = columncount;
  }
  
  public String getCurrentFile()
  {
    return this.reader.name();
  }
  
  public void setColumnOffset(int columnIndex, int columnOffset)
  {
    this.columnOffset[columnIndex] = columnOffset;
  }
  
  public void setColumnLength(int columnIndex, int columnLength)
  {
    this.columnLength[columnIndex] = columnLength;
  }
  
  public void update(Observable o, Object arg)
  {
    switch ((com.bloom.source.lib.constant.Constant.eventType)arg)
    {
    case ON_OPEN: 
      this.recordCheckpoint.setSourceName(reader().name());
      break;
    case ON_CLOSE: 
      this.recordCheckpoint.setSourceName("");
      break;
    }
  }
  
  public CheckpointDetail getCheckpointDetail()
  {
    CheckpointDetail checkpointDetail = new CheckpointDetail(this.recordCheckpoint);
    checkpointDetail.setBytesRead(this.reader.getCheckpointDetail().getBytesRead());
    return checkpointDetail;
  }
  
  public void positionToBeginning()
    throws AdapterException
  {
    reader().skipBytes(0L);
  }
  
  public void reset()
    throws AdapterException
  {}
  
  public MetaData getMetaData()
  {
    return null;
  }
  
  public Map<String, String> getColumnValueAsMap(int index)
    throws AdapterException
  {
    return null;
  }
  
  public List<String> applyRegexOnColumnValue(String columnValue, String regex)
  {
    return null;
  }
  
  public recordstatus getRecordStatus()
  {
    return null;
  }
  
  public String getErrorMessage()
  {
    return this.errorMessage;
  }
  
  public Date eventTime()
  {
    return null;
  }
  
  public void onEvent(ResetEvent event) {}
  
  public void onEvent(RowEvent event) {}
  
  public void onEvent(ColumnEvent event) {}
  
  public void onEvent(QuoteEvent event) {}
  
  public void onEvent(QuoteBeginEvent event) {}
  
  public void onEvent(QuoteEndEvent event) {}
  
  public boolean onEvent(SMEvent eventData)
  {
    return false;
  }
  
  public void onEvent(RowBeginEvent event) {}
  
  public void onEvent(RowEndEvent event) {}
  
  public void onEvent(TimeStampEvent event) {}
  
  public void onEvent(EndOfBlockEvent event) {}
  
  public void onEvent(EscapeEvent event) {}
  
  public void onEvent(CommentEvent event) {}
  
  public void onEvent(NVPEvent nvpEvent) {}
}
