package com.bloom.source.lib.socket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.intf.Notify;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;

public abstract class SocketReader
{
  static Logger logger = Logger.getLogger(SocketReader.class);
  private SocketChannel socketChannel = null;
  public String remoteAddress;
  public int serverPort;
  protected ByteBuffer byteBuffer;
  protected byte[] leftOverByteBuffer;
  long seekPosition = 0L;
  protected long connectionTimeout = 0L;
  protected int readTimeout = 0;
  protected long startTime = 0L;
  int blockSize = 0;
  Notify rs;
  Selector selector;
  Selector readSelector;
  public char ROW_DELIMITER = '\000';
  public char COL_DELIMITER = '\000';
  long bytesWritten = 0L;
  private boolean connected = false;
  private int connectionRetry = 0;
  
  public SocketReader(Notify rs, Property prop)
  {
    this.rs = rs;
    loadProperties(prop);
    
    this.byteBuffer = ByteBuffer.allocateDirect(2 * this.blockSize);
    this.leftOverByteBuffer = new byte[this.blockSize];
  }
  
  private void initializeSocketChannel()
    throws IOException
  {
    this.socketChannel = SocketChannel.open();
    this.socketChannel.configureBlocking(false);
    this.connected = this.socketChannel.connect(new InetSocketAddress(this.remoteAddress, this.serverPort));
    this.startTime = System.currentTimeMillis();
  }
  
  public void connect()
    throws AdapterException
  {
    try
    {
      long connectionAttempt = 0L;
      initializeSocketChannel();
      connectionAttempt += 1L;
      while (!this.connected) {
        try
        {
          while (System.currentTimeMillis() - this.startTime < this.connectionTimeout)
          {
            if (this.socketChannel.isConnected())
            {
              this.connected = true;
              this.socketChannel.finishConnect();
              break;
            }
            Thread.sleep(50L);
          }
          if (!(this.connected = this.socketChannel.finishConnect()))
          {
            AdapterException se = new AdapterException("Unable to connect host at " + this.remoteAddress + " at port no " + this.serverPort + ". Attempting retry");
            throw se;
          }
        }
        catch (AdapterException se)
        {
          if ((connectionAttempt <= this.connectionRetry) || (this.connectionRetry < 0))
          {
            initializeSocketChannel();
            connectionAttempt += 1L;
          }
          else
          {
            AdapterException e = new AdapterException(Error.CONNECTION_RETRY_EXCEEDED);
            throw e;
          }
        }
        catch (InterruptedException e)
        {
          AdapterException ie = new AdapterException(Error.GENERIC_INTERRUPT_EXCEPTION);
          throw ie;
        }
      }
      this.selector = Selector.open();
      this.readSelector = Selector.open();
      this.socketChannel.register(this.selector, 5);
      this.socketChannel.register(this.readSelector, 1);
    }
    catch (IOException e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      throw se;
    }
  }
  
  private void loadProperties(Property prop)
  {
    this.blockSize = (prop.blocksize * 1024);
    this.remoteAddress = prop.remoteaddress;
    this.serverPort = prop.portno;
    this.connectionTimeout = prop.connectionTimeout;
    this.readTimeout = prop.readTimeout;
    this.connectionRetry = prop.retryAttempt;
  }
  
  public void closeSocketReader()
    throws AdapterException
  {
    try
    {
      if (this.socketChannel != null)
      {
        this.socketChannel.close();
        this.socketChannel = null;
      }
      setCurrentSeekPosition(0L);
      if (this.rs != null) {
        this.rs.handleEvent(Constant.eventType.ON_CLOSE, null);
      }
    }
    catch (IOException e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION);
      logger.error(se.getMessage());
      throw se;
    }
  }
  
  public int getBlockSize()
  {
    return this.blockSize;
  }
  
  public Selector getSelector()
  {
    return this.selector;
  }
  
  public long getCurrentSeekPosition()
  {
    return this.seekPosition;
  }
  
  public void setCurrentSeekPosition(long seekOffset)
  {
    this.seekPosition = seekOffset;
  }
  
  public char[] getLeftOverBuffer()
  {
    return null;
  }
  
  public long readBlock()
    throws AdapterException
  {
    long readLen = -1L;
    
    getByteBuffer().clear();
    try
    {
      while (this.readSelector.select(this.readTimeout) > 0)
      {
        Set<SelectionKey> readyKeys = this.readSelector.selectedKeys();
        Iterator<SelectionKey> readyItor = readyKeys.iterator();
        while (readyItor.hasNext())
        {
          SelectionKey key = (SelectionKey)readyItor.next();
          readyItor.remove();
          ReadableByteChannel keyChannel = (SocketChannel)key.channel();
          if (key.isReadable())
          {
            readLen = keyChannel.read(this.byteBuffer);
            if (readLen <= 0L)
            {
              resetBuffer();
              return readLen;
            }
            if (readLen > 0L) {
              break;
            }
          }
        }
        if (readLen > 0L) {
          break;
        }
      }
    }
    catch (IOException e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      logger.error(se.getMessage());
      throw se;
    }
    setCurrentSeekPosition(getCurrentSeekPosition() + readLen);
    
    this.byteBuffer.flip();
    return readLen;
  }
  
  public void send(ByteBuffer dataToBeSent)
    throws AdapterException
  {
    try
    {
      if (getSelector().select(this.readTimeout) > 0)
      {
        Set<SelectionKey> readyKeys = getSelector().selectedKeys();
        Iterator<SelectionKey> readyItor = readyKeys.iterator();
        if (readyItor.hasNext())
        {
          SelectionKey key = (SelectionKey)readyItor.next();
          readyItor.remove();
          WritableByteChannel writeKeyChannel = (SocketChannel)key.channel();
          if (key.isWritable()) {
            while (dataToBeSent.hasRemaining()) {
              this.bytesWritten += writeKeyChannel.write(dataToBeSent);
            }
          }
        }
      }
    }
    catch (IOException e)
    {
      AdapterException se = new AdapterException(Error.HOST_CONNECTION_DROPPED, e);
      throw se;
    }
  }
  
  public long getBytesSent()
  {
    return this.bytesWritten;
  }
  
  private void resetBuffer()
  {
    getByteBuffer().position(0);
    getByteBuffer().limit(0);
  }
  
  public String getRemoteAddress()
  {
    return this.remoteAddress;
  }
  
  public void setRemoteAddress(String remoteAddress)
  {
    this.remoteAddress = remoteAddress;
  }
  
  public int getServerPort()
  {
    return this.serverPort;
  }
  
  public void setServerPort(int serverPort)
  {
    this.serverPort = serverPort;
  }
  
  public byte[] getLeftOverByteBuffer()
  {
    return this.leftOverByteBuffer;
  }
  
  public void close()
    throws AdapterException
  {
    closeSocketReader();
  }
  
  public ByteBuffer getByteBuffer()
  {
    return this.byteBuffer;
  }
  
  public SocketChannel getSocketChannel()
  {
    return this.socketChannel;
  }
}
