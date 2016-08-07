package com.bloom.source.lib.utils;

import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.socket.SocketReader;
import com.bloom.source.lib.type.EventStatus;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;

public class Forwarder
  extends SocketReader
{
  Property prop;
  Reader reader;
  Object data;
  ByteBuffer eventBuffer;
  byte[] serializedEventArray;
  static Logger logger = Logger.getLogger(Forwarder.class);
  static boolean closeConnection = false;
  
  public Forwarder(Property prop)
    throws AdapterException
  {
    super(null, prop);
    this.prop = prop;
    this.reader = Reader.FileReader(prop);
    this.reader.position(null, true);
  }
  
  public void connectToSubscriber()
    throws AdapterException
  {
    connect();
    if (logger.isInfoEnabled()) {
      logger.info("Connection to subscriber is successful at remote address " + this.prop.remoteaddress + " and port no " + this.prop.portno);
    }
  }
  
  public EventStatus getEventStatus()
    throws AdapterException
  {
    try
    {
      if ((this.data = this.reader.readBlock()) != null)
      {
        this.eventBuffer = ((ByteBuffer)this.data);
        return EventStatus.PUBLISH_EVENT;
      }
    }
    catch (AdapterException e)
    {
      close();
      throw e;
    }
    return EventStatus.NO_EVENT;
  }
  
  public void publishEvents()
    throws AdapterException
  {
    try
    {
      send(this.eventBuffer);
    }
    catch (AdapterException e)
    {
      close();
      throw e;
    }
    this.data = null;
    this.eventBuffer.clear();
  }
  
  public void closeSubscriberConnection()
    throws AdapterException
  {
    try
    {
      close();
      this.reader.close();
    }
    catch (IOException e)
    {
      AdapterException sourceException = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      throw sourceException;
    }
    if (logger.isInfoEnabled()) {
      logger.info("Connection with subscriber has been closed successfully at remote address " + this.prop.remoteaddress + " and port no " + this.prop.portno);
    }
  }
  
  public void receiveEvents() {}
  
  public static void main(String[] args)
  {
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      public void run()
      {
        Forwarder.closeConnection = true;
        if (Forwarder.logger.isInfoEnabled()) {
          Forwarder.logger.info("Forwarder has been shutdown successfully");
        }
      }
    });
    Properties configProperties = new Properties();
    String propertyFile;
    if (args.length > 0) {
      propertyFile = args[0];
    } else {
      propertyFile = "../conf/forwarder.properties";
    }
    InputStream propertyStream = null;
    try
    {
      propertyStream = new FileInputStream(new File(propertyFile));
    }
    catch (FileNotFoundException e)
    {
      AdapterException se = new AdapterException(Error.INVALID_DIRECTORY, e);
      logger.error(se.getErrorMessage() + ". Unable to locate configuration file needed by the forwarder.");
      System.exit(0);
    }
    try
    {
      configProperties.load(propertyStream);
    }
    catch (IOException e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      logger.error(se.getErrorMessage() + ". Problem loading configuration file.");
      System.exit(0);
    }
    Map<String, Object> map = new HashMap();
    for (String name : configProperties.stringPropertyNames()) {
      map.put(name, configProperties.getProperty(name));
    }
    Property prop = new Property(map);
    
    boolean reInitializeForwarder = false;
    do
    {
      try
      {
        Forwarder lf = new Forwarder(prop);
        lf.connectToSubscriber();
        for (;;)
        {
          if (lf.getEventStatus() != EventStatus.NO_EVENT) {
            lf.publishEvents();
          } else {
            try
            {
              Thread.sleep(prop.eofdelay);
            }
            catch (InterruptedException e)
            {
              AdapterException se = new AdapterException(Error.GENERIC_INTERRUPT_EXCEPTION, e);
              throw se;
            }
          }
          if (closeConnection) {
            break;
          }
        }
        lf.closeSubscriberConnection();
      }
      catch (AdapterException e)
      {
        logger.error(e.getErrorMessage());
        if (e.getType() == Error.HOST_CONNECTION_DROPPED)
        {
          if (logger.isInfoEnabled()) {
            logger.info("Reinitializing Forwarder. Attempting to connect back to host...");
          }
          reInitializeForwarder = true;
        }
      }
    } while (reInitializeForwarder);
  }
}
