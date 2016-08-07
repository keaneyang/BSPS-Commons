package com.bloom.source.lib.reader;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;

public class TCPReader
  extends WASocket
{
  private SocketChannel channel;
  private ServerSocketChannel serverSocket;
  private String serverHostName;
  private int listenPort;
  
  public TCPReader(Property prop)
    throws AdapterException
  {
    super(prop);
  }
  
  protected void init()
    throws AdapterException
  {
    super.init();
    this.listenPort = this.property.portno;
    this.serverHostName = this.property.ipaddress;
    if (this.listenPort != 0) {
      listen();
    } else {
      connect();
    }
  }
  
  public void listen()
    throws AdapterException
  {
    try
    {
      this.serverSocket = ServerSocketChannel.open();
      InetAddress serverAddr;
      if ((this.serverHostName == null) || (this.serverHostName == "")) {
        serverAddr = InetAddress.getLocalHost();
      } else {
        serverAddr = InetAddress.getByName(this.serverHostName);
      }
      InetSocketAddress addr = new InetSocketAddress(serverAddr, this.listenPort);
      this.serverSocket.bind(addr);
      this.serverSocket.configureBlocking(false);
    }
    catch (IOException e)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
    }
  }
  
  protected void registerWithSelector(Selector selector)
    throws AdapterException
  {
    try
    {
      this.serverSocket.register(selector, 16);
    }
    catch (ClosedChannelException e)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
    }
  }
  
  public Object readBlock()
    throws AdapterException
  {
    return readBlock(this.channel);
  }
  
  public void connect()
    throws AdapterException
  {
    System.out.println("Connecting to...");
    try
    {
      this.clientSocket = new Socket(this.serverIP, this.serverPort);
    }
    catch (UnknownHostException unknownHostExc)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION);
    }
    catch (IOException ioExc)
    {
      throw new AdapterException(Error.INVALID_IP_ADDRESS);
    }
  }
  
  public void close()
    throws IOException
  {
    if (this.clientSocket != null) {
      this.clientSocket.close();
    }
    if (this.serverSocket != null) {
      this.serverSocket.close();
    }
  }
  
  protected Object readBlock(SelectableChannel channel)
    throws AdapterException
  {
    SocketChannel sockChannel = (SocketChannel)channel;
    
    ByteBuffer buff = ByteBuffer.allocate(blockSize());
    try
    {
      int bytesRead = sockChannel.read(buff);
      if (bytesRead != -1)
      {
        buff.limit(bytesRead);
        buff.flip();
      }
      else
      {
        sockChannel.close();
        return null;
      }
    }
    catch (IOException exp)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION);
    }
    return buff;
  }
  
  public CheckpointDetail getCheckpointDetail()
  {
    return this.recoveryCheckpoint;
  }
}
