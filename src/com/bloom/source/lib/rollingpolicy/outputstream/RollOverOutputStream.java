package com.bloom.source.lib.rollingpolicy.outputstream;

import com.bloom.source.lib.intf.RollOverObserver;
import com.bloom.source.lib.rollingpolicy.util.RolloverFilenameFormat;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Vector;
import org.apache.log4j.Logger;

public abstract class RollOverOutputStream
  extends FilterOutputStream
{
  protected final RolloverFilenameFormat filenameFormat;
  private boolean changed = false;
  private Vector<RollOverObserver> observers;
  private OutputStreamBuilder outputStreamBuilder;
  private static final Logger logger = Logger.getLogger(RollOverOutputStream.class);
  private boolean isRollingOver = false;
  
  public RollOverOutputStream(OutputStreamBuilder outputStreamProvider, RolloverFilenameFormat filenameFormat)
    throws IOException
  {
    super(outputStreamProvider.buildOutputStream(filenameFormat));
    this.outputStreamBuilder = outputStreamProvider;
    this.filenameFormat = filenameFormat;
    this.observers = new Vector();
  }
  
  public void write(byte[] bytes)
    throws IOException
  {
    this.out.write(bytes);
  }
  
  public void registerObserver(RollOverObserver observer)
  {
    if (!this.observers.contains(observer)) {
      this.observers.addElement(observer);
    }
  }
  
  public void notifyObserversBeforeRollover()
    throws IOException
  {
    Object[] localArray = null;
    synchronized (this)
    {
      if (!this.changed) {
        return;
      }
      localArray = this.observers.toArray();
      this.changed = false;
    }
    for (int i = 0; i < localArray.length; i++) {
      ((RollOverObserver)localArray[i]).preRollover();
    }
  }
  
  public void notifyObserversAfterRollover(String filename)
    throws IOException
  {
    Object[] localArray = null;
    synchronized (this)
    {
      if (!this.changed) {
        return;
      }
      localArray = this.observers.toArray();
      this.changed = false;
    }
    for (int i = 0; i < localArray.length; i++) {
      ((RollOverObserver)localArray[i]).postRollover(filename);
    }
  }
  
  public synchronized void setChanged()
  {
    this.changed = true;
  }
  
  public synchronized void rollover()
    throws IOException
  {
    if (this.isRollingOver) {
      return;
    }
    this.isRollingOver = true;
    setChanged();
    notifyObserversBeforeRollover();
    OutputStream tobeClosed = this.out;
    tobeClosed.close();
    String lastRolledoverFile = this.filenameFormat.getCurrentFileName();
    this.out = this.outputStreamBuilder.buildOutputStream(this.filenameFormat);
    if (logger.isInfoEnabled()) {
      logger.info("Closing the file " + lastRolledoverFile + ". Opening a new file " + this.filenameFormat.getCurrentFileName());
    }
    setChanged();
    notifyObserversAfterRollover(lastRolledoverFile);
    this.isRollingOver = false;
  }
  
  public void close()
    throws IOException
  {
    setChanged();
    notifyObserversBeforeRollover();
    
    super.close();
    
    String lastRolledoverFile = this.filenameFormat.getCurrentFileName();
    setChanged();
    notifyObserversAfterRollover(lastRolledoverFile);
  }
  
  public static abstract interface OutputStreamBuilder
  {
    public abstract OutputStream buildOutputStream(RolloverFilenameFormat paramRolloverFilenameFormat)
      throws IOException;
  }
}
