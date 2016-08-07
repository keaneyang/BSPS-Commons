package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;

public class ThreadedQueueReader
  extends QueueReader
  implements Runnable
{
  private Selector selector;
  private Thread selectThread;
  private boolean doneWithThread;
  private final int QUEUE_BUFFER_LIMIT = 52428800;
  Logger logger = Logger.getLogger(ThreadedQueueReader.class);
  
  public ThreadedQueueReader(ReaderBase link)
    throws AdapterException
  {
    super(link);
    this.isThreaded = true;
  }
  
  protected void init()
    throws AdapterException
  {
    super.init();
    this.doneWithThread = false;
    this.selectThread = new Thread(this);
    this.selectThread.start();
  }
  
  public void run()
  {
    try
    {
      this.selector = Selector.open();
      
      this.linkedStrategy.registerWithSelector(this.selector);
      
      getClass();int maxQueueSize = 52428800 / 1024 / this.blockSize;
      while (!this.doneWithThread)
      {
        while ((!this.doneWithThread) && (this.msgQueue.size() > maxQueueSize)) {
          try
          {
            Thread.sleep(100L);
          }
          catch (InterruptedException e)
          {
            AdapterException se = new AdapterException(Error.GENERIC_INTERRUPT_EXCEPTION, e);
            this.logger.error(se.getErrorMessage());
          }
        }
        this.selector.select();
        Iterator<SelectionKey> itr = this.selector.selectedKeys().iterator();
        while (itr.hasNext())
        {
          SelectionKey event = (SelectionKey)itr.next();
          itr.remove();
          if (event.isAcceptable())
          {
            ServerSocketChannel serverChannel = (ServerSocketChannel)event.channel();
            SocketChannel clientSocket = serverChannel.accept();
            clientSocket.configureBlocking(false);
            clientSocket.register(this.selector, 1);
          }
          else if (event.isReadable())
          {
            Object buffer = this.linkedStrategy.readBlock(event.channel());
            if (buffer != null) {
              enqueue(buffer);
            } else {
              event.channel().close();
            }
          }
        }
      }
      this.linkedStrategy.close();
    }
    catch (AdapterException soExp)
    {
      this.logger.error(soExp.getErrorMessage());
    }
    catch (IOException e)
    {
      this.logger.error(e.getMessage());
    }
  }
  
  public void close()
    throws IOException
  {
    super.close();
    if ((this.selectThread != null) && (!this.doneWithThread))
    {
      this.doneWithThread = true;
      while (this.selectThread.getState() != Thread.State.TERMINATED) {
        try
        {
          this.selector.wakeup();
          Thread.sleep(100L);
        }
        catch (InterruptedException e)
        {
          this.logger.warn("Threaded close interrupted");
        }
      }
      closeClientConnection();
      this.selector.close();
    }
    else if ((this.doneWithThread) && 
      (this.logger.isDebugEnabled()))
    {
      this.logger.debug("close() is called on already closed adapter");
    }
  }
  
  private void closeClientConnection()
    throws IOException
  {
    Iterator<SelectionKey> itr = this.selector.keys().iterator();
    SelectionKey event = null;
    while (itr.hasNext())
    {
      event = (SelectionKey)itr.next();
      event.channel().close();
    }
  }
  
  public CheckpointDetail getCheckpointDetail()
  {
    return this.recoveryCheckpoint;
  }
}
