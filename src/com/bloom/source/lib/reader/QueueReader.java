package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueReader
  extends ReaderBase
{
  protected LinkedBlockingQueue<Object> msgQueue;
  
  protected QueueReader()
    throws AdapterException
  {
    super((ReaderBase)null);
  }
  
  public QueueReader(ReaderBase linkStrategy)
    throws AdapterException
  {
    super(linkStrategy);
  }
  
  protected void init()
    throws AdapterException
  {
    if ((this.upstream != null) && ((this.upstream instanceof QueueReader)))
    {
      LinkedBlockingQueue<Object> msgQueue = ((QueueReader)this.upstream).msgQueue;
      if (msgQueue != null)
      {
        this.msgQueue = msgQueue;
        this.hasQueue = true;
      }
    }
    if (!this.hasQueue)
    {
      this.msgQueue = new LinkedBlockingQueue();
      this.hasQueue = true;
    }
    super.init();
  }
  
  public Object readBlock()
    throws AdapterException
  {
    return this.msgQueue.poll();
  }
  
  public void close()
    throws IOException
  {
    if (this.msgQueue.size() > 0) {
      this.msgQueue.clear();
    }
    this.linkedStrategy.close();
  }
  
  protected void enqueue(Object obj)
  {
    this.msgQueue.add(obj);
  }
  
  public CheckpointDetail getCheckpointDetail()
  {
    return this.recoveryCheckpoint;
  }
}
