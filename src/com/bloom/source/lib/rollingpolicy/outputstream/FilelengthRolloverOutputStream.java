package com.bloom.source.lib.rollingpolicy.outputstream;

import com.bloom.source.lib.rollingpolicy.util.RolloverFilenameFormat;

import java.io.IOException;
import java.io.OutputStream;

public class FilelengthRolloverOutputStream
  extends RollOverOutputStream
{
  private final long rolloverTriggerLength;
  private long currentLength = 0L;
  
  public FilelengthRolloverOutputStream(RollOverOutputStream.OutputStreamBuilder outputStreamProvider, RolloverFilenameFormat filenameFormat, long rolloverTriggerLength)
    throws IOException
  {
    super(outputStreamProvider, filenameFormat);
    this.rolloverTriggerLength = rolloverTriggerLength;
  }
  
  public synchronized void write(int b)
    throws IOException
  {
    this.out.write(b);
    this.currentLength += 4L;
    if (this.currentLength >= this.rolloverTriggerLength) {
      rollover();
    }
  }
  
  public synchronized void write(byte[] bytes)
    throws IOException
  {
    this.out.write(bytes);
    this.currentLength += bytes.length;
    if (this.currentLength >= this.rolloverTriggerLength) {
      rollover();
    }
  }
  
  public synchronized void write(byte[] bytes, int off, int len)
    throws IOException
  {
    this.out.write(bytes, off, len);
    this.currentLength += len;
    if (this.currentLength >= this.rolloverTriggerLength) {
      rollover();
    }
  }
  
  public synchronized void rollover()
    throws IOException
  {
    this.currentLength = 0L;
    super.rollover();
  }
}
