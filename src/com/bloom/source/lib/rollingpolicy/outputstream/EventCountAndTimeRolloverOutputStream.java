package com.bloom.source.lib.rollingpolicy.outputstream;

import com.bloom.source.lib.rollingpolicy.util.RolloverFilenameFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.log4j.Logger;

public class EventCountAndTimeRolloverOutputStream
  extends RollOverOutputStream
{
  private static final Logger logger = Logger.getLogger(EventCountAndTimeRolloverOutputStream.class);
  private final long maxEventCount;
  private long currentEventCounter = 0L;
  private final long rotationTimeMillis;
  private Timer rollOverTimer;
  
  public EventCountAndTimeRolloverOutputStream(RollOverOutputStream.OutputStreamBuilder builder, RolloverFilenameFormat format, long maxEventCount, long rotationTime)
    throws IOException
  {
    super(builder, format);
    this.maxEventCount = maxEventCount;
    this.rotationTimeMillis = rotationTime;
    this.rollOverTimer = new Timer("EventCountAndTimeRolloverOutputStream");
    this.rollOverTimer.schedule(new TimerTask()
    {
      public void run()
      {
        try
        {
          if (EventCountAndTimeRolloverOutputStream.this.currentEventCounter > 0L) {
            EventCountAndTimeRolloverOutputStream.this.rollover();
          }
        }
        catch (IOException e)
        {
          EventCountAndTimeRolloverOutputStream.logger.error("Failure in performing roll over", e);
        }
      }
    }, this.rotationTimeMillis, this.rotationTimeMillis);
  }
  
  public synchronized void write(int b)
    throws IOException
  {
    this.out.write(b);
    this.currentEventCounter += 1L;
    if (this.currentEventCounter >= this.maxEventCount) {
      rollover();
    }
  }
  
  public synchronized void write(byte[] bytes)
    throws IOException
  {
    this.out.write(bytes);
    this.currentEventCounter += 1L;
    if (this.currentEventCounter >= this.maxEventCount) {
      rollover();
    }
  }
  
  public synchronized void write(byte[] bytes, int off, int len)
    throws IOException
  {
    this.out.write(bytes, off, len);
    this.currentEventCounter += 1L;
    if (this.currentEventCounter >= this.maxEventCount) {
      rollover();
    }
  }
  
  public void rollover()
    throws IOException
  {
    super.rollover();
    this.currentEventCounter = 0L;
    this.rollOverTimer.cancel();
    this.rollOverTimer = new Timer("EventCountAndTimeRolloverOutputStream");
    this.rollOverTimer.schedule(new TimerTask()
    {
      public void run()
      {
        try
        {
          if (EventCountAndTimeRolloverOutputStream.this.currentEventCounter > 0L) {
            EventCountAndTimeRolloverOutputStream.this.rollover();
          }
        }
        catch (IOException e)
        {
          EventCountAndTimeRolloverOutputStream.logger.error("Failure in performing roll over", e);
        }
      }
    }, this.rotationTimeMillis, this.rotationTimeMillis);
  }
  
  public synchronized void close()
    throws IOException
  {
    super.close();
    this.rollOverTimer.cancel();
  }
}
