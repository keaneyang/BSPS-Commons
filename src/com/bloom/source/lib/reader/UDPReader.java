package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import org.apache.log4j.Logger;

public class UDPReader
  extends WASocket
{
  private String serverName;
  private int serverPort;
  private ByteBuffer buffer;
  private DatagramChannel socketChannel;
  private Logger logger = Logger.getLogger(UDPReader.class);
  
  public UDPReader(Property prop)
    throws AdapterException
  {
    super(prop);
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("UDPReader is initialized with following properties\nIPAddress - [" + prop.ipaddress + "]\n" + "PortNo - [" + prop.portno + "]\n" + "BlockSize - [" + prop.blocksize + "]");
    }
  }
  
  public void init()
    throws AdapterException
  {
    super.init();
    loadConfig();
    try
    {
      InetSocketAddress sockAddr = new InetSocketAddress(this.serverName, this.serverPort);
      this.socketChannel = DatagramChannel.open();
      this.socketChannel.socket().setReuseAddress(true);
      this.socketChannel.socket().bind(sockAddr);
      this.socketChannel.configureBlocking(false);
      if (this.logger.isTraceEnabled()) {
        this.logger.trace("UPD Server is initialized using IPAddress " + this.serverName + " , listening at " + this.serverPort);
      }
    }
    catch (SocketException sockExp)
    {
      throw new AdapterException(Error.GENERIC_EXCEPTION, "Got SocketException", sockExp);
    }
    catch (SecurityException securityExp)
    {
      throw new AdapterException(Error.GENERIC_EXCEPTION, "Got SecurityException", securityExp);
    }
    catch (IOException ioExp)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION, ioExp);
    }
  }
  
  private void loadConfig()
  {
    this.serverName = property().ipaddress;
    this.serverPort = property().portno;
    
    name("UDP:" + this.serverName + ":" + this.serverPort);
  }
  
  public Object readBlock()
    throws AdapterException
  {
    return readBlock(this.socketChannel);
  }
  
  protected Object readBlock(SelectableChannel channel)
    throws AdapterException
  {
    DatagramChannel udpChannel = (DatagramChannel)channel;
    
    this.buffer = ByteBuffer.allocate(blockSize());
    this.buffer.clear();
    try
    {
      udpChannel.receive(this.buffer);
    }
    catch (IOException exp)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION, exp);
    }
    this.buffer.flip();
    if (this.logger.isTraceEnabled()) {
      try
      {
        this.logger.trace("UDPReader has read " + this.buffer.limit() + " bytes from " + udpChannel.getRemoteAddress());
      }
      catch (IOException e)
      {
        throw new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      }
    }
    return this.buffer;
  }
  
  public void close()
    throws IOException
  {
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("UDPReader is closed and disconnected from " + this.socketChannel.getRemoteAddress());
    }
    this.socketChannel.close();
  }
  
  public void connect(InetSocketAddress addr)
    throws AdapterException
  {
    try
    {
      this.socketChannel = DatagramChannel.open();
      this.socketChannel.connect(addr);
      if (this.logger.isTraceEnabled()) {
        this.logger.trace("UDP client is initialized to send datagram to Server with IPAddress " + addr.getHostString() + " listening at " + addr.getPort());
      }
    }
    catch (UnknownHostException unknowHostExc)
    {
      throw new AdapterException(Error.INVALID_IP_ADDRESS, unknowHostExc);
    }
    catch (IOException ioExc)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION, ioExc);
    }
  }
  
  protected void registerWithSelector(Selector selector)
    throws AdapterException
  {
    try
    {
      this.socketChannel.register(selector, 1);
    }
    catch (ClosedChannelException e)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
    }
  }
  
  public CheckpointDetail getCheckpointDetail()
  {
    return this.recoveryCheckpoint;
  }
}
