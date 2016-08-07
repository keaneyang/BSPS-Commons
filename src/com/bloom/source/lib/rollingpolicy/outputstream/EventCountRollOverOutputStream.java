package com.bloom.source.lib.rollingpolicy.outputstream;

import com.bloom.source.lib.rollingpolicy.util.RolloverFilenameFormat;

import java.io.IOException;
import java.io.OutputStream;

public class EventCountRollOverOutputStream
  extends RollOverOutputStream
{
  private final long maxEventCount;
  private long currentEventCounter = 0L;
  
  public EventCountRollOverOutputStream(RollOverOutputStream.OutputStreamBuilder outputStreamProvider, RolloverFilenameFormat filenameFormat, long maxEventCount, boolean dataHasHeader)
    throws IOException
  {
    super(outputStreamProvider, filenameFormat);
    this.maxEventCount = maxEventCount;
    if (dataHasHeader) {
      this.currentEventCounter = -1L;
    }
  }
  
  public synchronized void write(int b)
    throws IOException
  {
    this.out.write(b);
    this.currentEventCounter += 1L;
    if (this.currentEventCounter == this.maxEventCount) {
      rollover();
    }
  }
  
  public synchronized void write(byte[] bytes)
    throws IOException
  {
    this.out.write(bytes);
    this.currentEventCounter += 1L;
    if (this.currentEventCounter == this.maxEventCount) {
      rollover();
    }
  }
  
  public synchronized void write(byte[] bytes, int off, int len)
    throws IOException
  {
    this.out.write(bytes, off, len);
    this.currentEventCounter += 1L;
    if (this.currentEventCounter == this.maxEventCount) {
      rollover();
    }
  }
  
  public void rollover()
    throws IOException
  {
    super.rollover();
    this.currentEventCounter = 0L;
  }
}
