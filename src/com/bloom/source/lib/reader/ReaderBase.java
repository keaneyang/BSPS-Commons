package com.bloom.source.lib.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.locks.LockSupport;

import org.apache.log4j.Logger;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;

public abstract class ReaderBase
  extends Observable
{
  private final int block = 65536;
  protected ReaderBase linkedStrategy;
  protected int blockSize;
  protected boolean isThreaded;
  protected boolean hasQueue;
  protected Property property;
  protected String name = "";
  protected ByteBuffer internalBuffer;
  protected boolean stopRead = false;
  protected String identifier;
  protected long bytesToSkip = 0L;
  protected CheckpointDetail recoveryCheckpoint;
  protected boolean supportsMutipleEndpoint;
  protected InputStream inputStream;
  protected ReaderBase upstream;
  private Logger logger = Logger.getLogger(ReaderBase.class);
  int retryCount = 0;
  protected boolean dontBlockOnEOF;
  protected boolean breakOnNoRecord;
  protected Map<String, Object> eventMetadataMap = new HashMap();
  
  protected ReaderBase(Property prop)
    throws AdapterException
  {
    this.property = prop;
    this.blockSize = (this.property.blocksize * 1024);
    this.breakOnNoRecord = this.property.getBoolean("breakonnorecord", false);
    this.dontBlockOnEOF = this.property.dontBlockOnEOF;
  }
  
  protected ReaderBase(ReaderBase link)
    throws AdapterException
  {
    this.linkedStrategy = link;
    if (link != null)
    {
      this.blockSize = this.linkedStrategy.blockSize();
      link.upstream(this);
      if (this.logger.isTraceEnabled()) {
        this.logger.trace("Reader " + name() + " is linked with the following strategy " + link.name());
      }
      this.breakOnNoRecord = this.linkedStrategy.breakOnNoRecord();
    }
  }
  
  protected void init(Property prop)
    throws AdapterException
  {
    this.property = prop;
    this.blockSize = (this.property.blocksize * 1024);
    this.dontBlockOnEOF = this.property.dontBlockOnEOF;
    
    init();
  }
  
  protected void init()
    throws AdapterException
  {
    if (this.linkedStrategy != null) {
      this.linkedStrategy.init();
    }
    init(false);
  }
  
  protected void init(boolean forwardTheCall)
    throws AdapterException
  {
    if (this.blockSize == 0) {
      this.blockSize = 65536;
    }
    this.internalBuffer = ByteBuffer.allocate(blockSize());
    this.internalBuffer.flip();
    this.recoveryCheckpoint = new CheckpointDetail();
  }
  
  public String name()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.name();
    }
    return this.name;
  }
  
  protected void name(String name)
  {
    if (this.linkedStrategy != null) {
      this.linkedStrategy.name(name);
    } else {
      this.name = name;
    }
  }
  
  public int blockSize()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.blockSize();
    }
    return this.blockSize;
  }
  
  protected void blockSize(int size)
  {
    this.blockSize = size;
  }
  
  public Property property()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.property();
    }
    return this.property;
  }
  
  public long skip(long bytes)
    throws IOException
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.skip(this.bytesToSkip);
    }
    this.bytesToSkip = bytes;
    return this.bytesToSkip;
  }
  
  public InputStream getInputStream()
    throws AdapterException
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.getInputStream();
    }
    return this.inputStream;
  }
  
  public long bytesToSkip()
  {
    return this.bytesToSkip;
  }
  
  public String CharSet()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.CharSet();
    }
    if (this.property != null) {
      return this.property.charset;
    }
    return null;
  }
  
  protected void registerWithSelector(Selector selector)
    throws AdapterException
  {}
  
  protected Object readBlock(SelectableChannel sc)
    throws AdapterException
  {
    return null;
  }
  
  protected void enqueue(Object obj) {}
  
  public abstract Object readBlock()
    throws AdapterException;
  
  public Object readBlock(boolean multiEndpointSupport)
    throws AdapterException
  {
    return null;
  }
  
  public void close()
    throws IOException
  {
    this.stopRead = true;
    if (this.linkedStrategy != null)
    {
      this.linkedStrategy.close();
      this.linkedStrategy = null;
    }
    if (this.internalBuffer != null) {
      this.internalBuffer = null;
    }
  }
  
  public int available()
    throws IOException
  {
    try
    {
      if (this.internalBuffer == null) {
        this.internalBuffer = ((ByteBuffer)readBlock());
      }
      if (this.internalBuffer != null)
      {
        if (this.internalBuffer.limit() - this.internalBuffer.position() == 0)
        {
          this.internalBuffer = ((ByteBuffer)readBlock());
          if (this.internalBuffer == null) {
            return 0;
          }
        }
        return this.internalBuffer.limit() - this.internalBuffer.position();
      }
    }
    catch (AdapterException e)
    {
      this.logger.error(e);
    }
    return 0;
  }
  
  private void retryWait()
  {
    if (this.retryCount < 10000)
    {
      this.retryCount += 1;
    }
    else if (this.retryCount < 20000)
    {
      Thread.yield();
      this.retryCount += 1;
    }
    else
    {
      LockSupport.parkNanos(this.retryCount);
      if (this.retryCount < 100000000) {
        this.retryCount += 10000;
      }
    }
  }
  
  public int read()
    throws IOException
  {
    if ((this.internalBuffer != null) && (this.internalBuffer.position() < this.internalBuffer.limit())) {
      return this.internalBuffer.get() & 0xFF;
    }
    ByteBuffer tmp = null;
    this.retryCount = 0;
    do
    {
      try
      {
        tmp = (ByteBuffer)readBlock();
        if (tmp != null)
        {
          this.internalBuffer = tmp;
          this.retryCount = 0;
          return this.internalBuffer.get() & 0xFF;
        }
        if (this.breakOnNoRecord) {
          throw new RuntimeException(new RecordException(RecordException.Type.NO_RECORD));
        }
        retryWait();
        if ((this.stopRead) || ((this.dontBlockOnEOF) && (this.retryCount > 30000))) {
          return -1;
        }
      }
      catch (AdapterException exp)
      {
        exp.printStackTrace();
      }
    } while (tmp == null);
    return 0;
  }
  
  public int read(byte[] buffer)
    throws AdapterException
  {
    return read(buffer, 0, buffer.length);
  }
  
  public int read(byte[] buffer, int off, int len)
  {
    int size = 0;
    if (len == 0) {
      return 0;
    }
    if (this.internalBuffer != null) {
      size = this.internalBuffer.limit() - this.internalBuffer.position();
    }
    if (size == 0)
    {
      this.retryCount = 0;
      do
      {
        try
        {
          this.internalBuffer = ((ByteBuffer)readBlock());
          if (this.internalBuffer == null)
          {
            if (this.breakOnNoRecord) {
              throw new RuntimeException(new RecordException(RecordException.Type.NO_RECORD));
            }
            retryWait();
            if ((this.stopRead) || ((this.dontBlockOnEOF) && (this.retryCount > 30000))) {
              return -1;
            }
          }
          if (this.logger.isDebugEnabled()) {
            try
            {
              if (this.internalBuffer != null)
              {
                byte[] tmp = new byte[this.internalBuffer.limit() + 1];
                System.arraycopy(this.internalBuffer.array(), 0, tmp, 0, this.internalBuffer.limit());
                this.logger.debug("Data : [" + new String(tmp, "UTF-8") + "]");
              }
            }
            catch (Exception e)
            {
              e.printStackTrace();
            }
          }
        }
        catch (AdapterException sExp)
        {
          return -1;
        }
      } while ((this.internalBuffer == null) && (!Thread.currentThread().isInterrupted()));
      if (this.internalBuffer != null) {
        size = this.internalBuffer.limit() - this.internalBuffer.position();
      }
    }
    if (len < size) {
      size = len;
    }
    if (this.internalBuffer != null)
    {
      System.arraycopy(this.internalBuffer.array(), this.internalBuffer.position(), buffer, off, size);
      
      this.internalBuffer.position(this.internalBuffer.position() + size);
    }
    this.retryCount = 0;
    return size;
  }
  
  public void registerObserver(Object observer)
  {
    addObserver((Observer)observer);
  }
  
  public long getEOFPosition()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.getEOFPosition();
    }
    return 0L;
  }
  
  public void position(CheckpointDetail recordAttribute, boolean position)
    throws AdapterException
  {
    if (this.linkedStrategy != null) {
      this.linkedStrategy.position(recordAttribute, position);
    }
  }
  
  public long skipBytes(long offset)
    throws AdapterException
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.skipBytes(offset);
    }
    return 0L;
  }
  
  public CheckpointDetail getCheckpointDetail()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.getCheckpointDetail();
    }
    return this.recoveryCheckpoint;
  }
  
  public void setCheckPointDetails(CheckpointDetail cp)
  {
    if (this.linkedStrategy != null) {
      this.linkedStrategy.setCheckPointDetails(cp);
    } else {
      this.recoveryCheckpoint = cp;
    }
  }
  
  public boolean breakOnNoRecord()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.breakOnNoRecord();
    }
    return this.breakOnNoRecord;
  }
  
  public String identifier()
  {
    if (this.identifier != null) {
      return this.identifier;
    }
    return this.linkedStrategy.identifier();
  }
  
  public boolean supportsMutipleEndpoint()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.supportsMutipleEndpoint();
    }
    return this.supportsMutipleEndpoint;
  }
  
  protected void onClose(Object identifier)
  {
    if (this.upstream != null)
    {
      this.upstream.onClose(Constant.eventType.ON_CLOSE);
    }
    else
    {
      setChanged();
      notifyObservers(identifier);
    }
  }
  
  protected void onOpen(Object identifier)
  {
    if (this.upstream != null)
    {
      this.upstream.onOpen(Constant.eventType.ON_OPEN);
    }
    else
    {
      setChanged();
      notifyObservers(identifier);
    }
  }
  
  public void setChangedFlag()
  {
    setChanged();
  }
  
  public void upstream(ReaderBase up)
  {
    this.upstream = up;
  }
  
  public void downstream(ReaderBase down)
  {
    this.linkedStrategy = down;
  }
  
  public int eofdelay()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.eofdelay();
    }
    return property().eofdelay;
  }
  
  public boolean markSupported()
  {
    return false;
  }
  
  public void setInputStream(InputStream dataSource)
    throws AdapterException
  {
    if (this.linkedStrategy != null) {
      this.linkedStrategy.setInputStream(dataSource);
    }
  }
  
  public void setCharacterBuffer(CharBuffer buff)
  {
    if (this.linkedStrategy != null) {
      this.linkedStrategy.setCharacterBuffer(buff);
    }
  }
  
  public Map<String, Object> getEventMetadata()
  {
    if (this.linkedStrategy != null) {
      this.eventMetadataMap = this.linkedStrategy.getEventMetadata();
    }
    return this.eventMetadataMap;
  }
}
