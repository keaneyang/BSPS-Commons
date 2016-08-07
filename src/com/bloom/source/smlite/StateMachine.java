package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;
import com.bloom.source.lib.reader.Reader;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import java.nio.CharBuffer;
import org.apache.log4j.Logger;

public class StateMachine
{
  protected SMProperty prop;
  protected SMCallback callback;
  protected CharParser parser;
  protected Reader dataSource;
  boolean canBreak;
  boolean canFlushBuffer;
  boolean hasRecordBegin;
  boolean supportsMutipleEndpoint;
  protected char[] bufferArray;
  char[] tmpArray;
  int bufferOffset;
  int bufferLimit;
  int blockSize;
  boolean inQuote;
  SMEvent[] eventStack;
  RowEvent rEvent;
  RowEvent endOfBlock;
  ResetEvent resetEvent;
  int maxDelimiterLength;
  int lastRecordEndPosition;
  CharBuffer internalBuffer;
  Logger logger = Logger.getLogger(StateMachine.class);
  boolean duplicateEndOfBlockEvent;
  boolean raiseEndOfBlockEvent;
  
  public StateMachine(Reader reader, SMProperty _prop)
  {
    this.dataSource = reader;
    this.prop = _prop;
  }
  
  protected void init()
  {
    this.bufferOffset = 0;
    this.bufferLimit = 0;
    this.internalBuffer = CharBuffer.allocate(this.dataSource.blockSize() * 2);
    this.bufferArray = this.internalBuffer.array();
    
    this.lastRecordEndPosition = 0;
    
    this.blockSize = (this.dataSource.blockSize() * 2);
    this.supportsMutipleEndpoint = this.dataSource.supportsMutipleEndpoint();
    
    this.duplicateEndOfBlockEvent = false;
    
    this.resetEvent = new ResetEvent();
    
    initializeStateObjs();
  }
  
  private void initializeStateObjs()
  {
    this.eventStack = new SMEvent[13];
    
    this.rEvent = new RowEvent(this.bufferArray);
    this.eventStack[2] = this.rEvent;
    
    this.eventStack[1] = EventFactory.createEvent(1);
    this.eventStack[3] = EventFactory.createEvent(3);
    this.eventStack[4] = EventFactory.createEvent(4);
    this.eventStack[5] = EventFactory.createEvent(5);
    this.eventStack[0] = EventFactory.createEvent(0);
    this.eventStack[10] = EventFactory.createEvent(10);
    this.eventStack[8] = EventFactory.createEvent(8);
    this.eventStack[6] = EventFactory.createEvent(6);
    if (this.prop.blockAsCompleteRecord)
    {
      this.endOfBlock = new RowEvent(this.bufferArray);
      this.endOfBlock.length(0);
      this.eventStack[7] = this.endOfBlock;
    }
    SMEvent nvpEvent = new NVPEvent(this.bufferArray);
    this.eventStack[12] = nvpEvent;
  }
  
  public StateMachine(SMProperty _prop)
  {
    this.prop = _prop;
  }
  
  public StateMachine(SMProperty _prop, SMCallback _callback)
  {
    this.prop = _prop;
    this.callback = _callback;
  }
  
  public void callback(SMCallback _callback)
  {
    this.callback = _callback;
  }
  
  public Reader reader()
  {
    return this.dataSource;
  }
  
  public void reader(Reader reader)
  {
    this.dataSource = reader;
  }
  
  public void parser(CharParser cParser)
  {
    this.parser = cParser;
    this.hasRecordBegin = this.parser.hasRecordBeginSupport();
    this.maxDelimiterLength = this.parser.maxDelimiterLength();
  }
  
  public void lastRecordEndPosition(int offset)
  {
    this.lastRecordEndPosition = offset;
  }
  
  public int lastRecordEndPosition()
  {
    return this.lastRecordEndPosition;
  }
  
  int trashDataLen = 0;
  
  protected boolean resetInternalBuffer()
    throws RecordException, AdapterException
  {
    if (this.prop.blockAsCompleteRecord)
    {
      if (this.raiseEndOfBlockEvent)
      {
        this.raiseEndOfBlockEvent = false;
        return true;
      }
      this.bufferLimit = 0;
    }
    else
    {
      if ((this.hasRecordBegin) && (this.canFlushBuffer) && 
        (this.lastRecordEndPosition == 0) && (this.bufferOffset > 0))
      {
        if (this.bufferLimit > this.maxDelimiterLength - 1) {
          this.lastRecordEndPosition = (this.bufferOffset - this.maxDelimiterLength - 1);
        }
        this.trashDataLen = this.lastRecordEndPosition;
      }
      if (this.lastRecordEndPosition > 0)
      {
        int leftout = this.bufferOffset - this.lastRecordEndPosition;
        this.resetEvent.currentPosition = this.bufferOffset;
        if (leftout > 0)
        {
          if (leftout < this.blockSize)
          {
            System.arraycopy(this.bufferArray, this.lastRecordEndPosition, this.bufferArray, 0, leftout);
            this.bufferLimit = leftout;
            this.lastRecordEndPosition = 0;
            this.bufferOffset = 0;
          }
          else
          {
            this.logger.error("Running out of buffer, data-source: {" + this.dataSource.name() + "}");
            throw new AdapterException("Running out of buffer, data-source: {" + this.dataSource.name() + "}");
          }
        }
        else
        {
          if ((this.hasRecordBegin) && (!this.canFlushBuffer)) {
            this.bufferOffset = 0;
          }
          this.bufferLimit = 0;
        }
      }
      else if (this.bufferLimit >= this.blockSize / 2)
      {
        this.logger.warn("Record seems to be larger than internal buffer size {" + this.blockSize + "}, going to double it {" + this.blockSize * 2 + "}");
        this.blockSize *= 2;
        CharBuffer tmpBuffer = CharBuffer.allocate(this.blockSize);
        this.bufferArray = this.internalBuffer.array();
        System.arraycopy(this.bufferArray, 0, tmpBuffer.array(), 0, this.bufferLimit);
        this.internalBuffer = tmpBuffer;
        this.bufferArray = this.internalBuffer.array();
      }
    }
    CharBuffer data = (CharBuffer)this.dataSource.readBlock();
    if (data != null)
    {
      this.raiseEndOfBlockEvent = true;
      System.arraycopy(data.array(), 0, this.bufferArray, this.bufferLimit, data.limit());
      this.bufferLimit += data.limit();
      this.lastRecordEndPosition = 0;
      ignoreEvents(null);
      this.resetEvent.currentPosition = this.trashDataLen;
      this.trashDataLen = 0;
      this.resetEvent.buffer = this.bufferArray;
      publishEvent(this.resetEvent);
      this.parser.reset();
      this.bufferOffset = 0;
      return false;
    }
    if (this.hasRecordBegin) {
      this.bufferOffset = 0;
    }
    if (this.noRecordException == null) {
      this.noRecordException = new RecordException(RecordException.Type.NO_RECORD);
    }
    throw this.noRecordException;
  }
  
  RecordException noRecordException = null;
  
  public char getChar()
    throws RecordException, AdapterException
  {
    if ((this.bufferOffset >= this.bufferLimit) && 
      (resetInternalBuffer())) {
      return '\000';
    }
    return this.bufferArray[(this.bufferOffset++)];
  }
  
  public void rewind(int offset)
  {
    if (this.bufferOffset - offset < 0)
    {
      this.logger.error("Trying to rewind beyond the limit (bufferOffset:" + this.bufferOffset + " offset:" + offset + ")");
      this.bufferOffset = 0;
    }
    else
    {
      this.bufferOffset -= offset;
    }
  }
  
  public void next()
    throws RecordException, AdapterException
  {
    this.parser.next();
  }
  
  public void publishEvent(short s)
  {
    SMEvent eventData = this.eventStack[s];
    if (eventData != null)
    {
      eventData.position(this.bufferOffset);
      if (eventData.state() == 2)
      {
        ((RowEvent)this.eventStack[s]).rowBegin(this.lastRecordEndPosition);
        this.lastRecordEndPosition = eventData.position();
      }
      eventData.publishEvent(this.callback);
    }
  }
  
  public void publishEvent(ResetEvent event)
  {
    event.publishEvent(this.callback);
  }
  
  public void publishEvent(RowEvent event)
  {
    event.rowBegin(this.lastRecordEndPosition);
    this.lastRecordEndPosition = event.position();
    event.buffer = this.bufferArray;
    publishEvent(event);
  }
  
  public void publishEvent(SMEvent event)
  {
    event.position(this.bufferOffset);
    if (event.state() == 2)
    {
      ((RowEvent)event).rowBegin(this.lastRecordEndPosition);
      ((RowEvent)event).buffer = this.bufferArray;
      this.lastRecordEndPosition = event.position();
    }
    event.publishEvent(this.callback);
  }
  
  public void ignoreEvents(short[] event)
  {
    this.parser.ignoreEvents(event);
  }
  
  public void ignoreEvents(short[] events, SMEvent excludeEvent)
  {
    this.parser.ignoreEvents(events, excludeEvent);
  }
  
  public void reset(boolean clearBuffer)
  {
    ignoreEvents(null);
    if (clearBuffer)
    {
      this.internalBuffer.clear();
      this.internalBuffer.flip();
      
      this.lastRecordEndPosition = 0;
      this.bufferOffset = 0;
      this.bufferLimit = 0;
    }
  }
  
  public CharParser createParser(SMProperty smProperty)
    throws AdapterException
  {
    return new CharParserLite(this, smProperty);
  }
  
  public static boolean canHandle(com.bloom.source.sm.SMProperty smProperty)
  {
    return true;
  }
  
  protected boolean validateProperty()
  {
    return true;
  }
  
  public char[] buffer()
  {
    return this.bufferArray;
  }
  
  public boolean canBreak()
  {
    return this.canBreak;
  }
  
  public void canBreak(boolean org)
  {
    this.canBreak = org;
  }
  
  public void canFlush(boolean flush)
  {
    this.canFlushBuffer = flush;
  }
  
  public void close()
  {
    this.parser.close();
  }
}
