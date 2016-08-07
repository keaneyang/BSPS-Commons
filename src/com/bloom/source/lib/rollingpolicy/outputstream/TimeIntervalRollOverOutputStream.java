package com.bloom.source.lib.rollingpolicy.outputstream;

import com.bloom.source.lib.rollingpolicy.util.RolloverFilenameFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.log4j.Logger;

public class TimeIntervalRollOverOutputStream
  extends RollOverOutputStream
{
  private long rotationTimeMillis;
  private Timer rollOverTimer;
  private long eventCounter = 0L;
  private Logger logger = Logger.getLogger(TimeIntervalRollOverOutputStream.class);
  private int minimumEventCountToTriggerRollOver = 0;
  
  public TimeIntervalRollOverOutputStream(RollOverOutputStream.OutputStreamBuilder outputStreamProvider, RolloverFilenameFormat filenameFormat, long maxRotationTime, boolean dataHasHeader)
    throws IOException
  {
    super(outputStreamProvider, filenameFormat);
    this.rotationTimeMillis = maxRotationTime;
    this.rollOverTimer = new Timer("TimeIntervalRollOverOutputStream");
    if (dataHasHeader) {
      this.minimumEventCountToTriggerRollOver = 1;
    }
    this.rollOverTimer.schedule(new TimerTask()
    {
      public void run()
      {
        try
        {
          if (TimeIntervalRollOverOutputStream.this.eventCounter > TimeIntervalRollOverOutputStream.this.minimumEventCountToTriggerRollOver)
          {
            TimeIntervalRollOverOutputStream.this.rollover();
            TimeIntervalRollOverOutputStream.this.eventCounter = TimeIntervalRollOverOutputStream.this.minimumEventCountToTriggerRollOver;
          }
        }
        catch (IOException e)
        {
          TimeIntervalRollOverOutputStream.this.logger.error("Failure in performing roll over", e);
        }
      }
    }, this.rotationTimeMillis, this.rotationTimeMillis);
  }
  
  public synchronized void write(int b)
    throws IOException
  {
    this.out.write(b);
    this.eventCounter += 1L;
  }
  
  public synchronized void write(byte[] bytes)
    throws IOException
  {
    this.out.write(bytes);
    this.eventCounter += 1L;
  }
  
  public synchronized void write(byte[] bytes, int off, int len)
    throws IOException
  {
    this.out.write(bytes, off, len);
    this.eventCounter += 1L;
  }
  
  public synchronized void close()
    throws IOException
  {
    super.close();
    this.rollOverTimer.cancel();
  }
}
