package com.bloom.source.lib.reader;

import com.bloom.source.lib.io.common.JMSCommon;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import org.apache.log4j.Logger;

public class JMSReader
  extends QueueReader
  implements MessageListener
{
  private JMSCommon jmsCommon;
  private Logger logger = Logger.getLogger(JMSReader.class);
  
  public JMSReader(Property prop)
    throws AdapterException
  {
    this.property = prop;
    this.jmsCommon = new JMSCommon(this.property);
  }
  
  private void initializeTopic()
    throws AdapterException
  {
    try
    {
      Topic topic = this.jmsCommon.retrieveTopicInstance();
      TopicSubscriber subscriber = ((TopicSession)this.jmsCommon.getSession()).createSubscriber(topic);
      subscriber.setMessageListener(this);
      this.jmsCommon.startConnection();
      name(this.property.topic);
      if (this.logger.isTraceEnabled()) {
        this.logger.trace("JMS topic " + this.property.topic + " has been initialized");
      }
    }
    catch (JMSException e)
    {
      throw new AdapterException(Error.GENERIC_EXCEPTION, e);
    }
  }
  
  private void initializeQueue()
    throws AdapterException
  {
    try
    {
      Queue msgQueue = this.jmsCommon.retrieveQueueInstance();
      QueueReceiver receiver = ((QueueSession)this.jmsCommon.getSession()).createReceiver(msgQueue);
      receiver.setMessageListener(this);
      this.jmsCommon.startConnection();
      name(this.property.queueName);
      if (this.logger.isTraceEnabled()) {
        this.logger.trace("JMS queue " + this.property.queueName + " has been intiialized");
      }
    }
    catch (JMSException e)
    {
      throw new AdapterException(Error.GENERIC_EXCEPTION, e);
    }
  }
  
  protected void init()
    throws AdapterException
  {
    blockSize(this.property.blocksize * 1024);
    super.init();
    this.jmsCommon.initialize();
    if ((this.property.topic != null) && (!this.property.topic.isEmpty())) {
      initializeTopic();
    } else if ((this.property.queueName != null) && (!this.property.queueName.trim().isEmpty())) {
      initializeQueue();
    } else {
      throw new AdapterException("Please provide a topic or queue name to read messages");
    }
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("JMSReader is initialized with following properties\nUserName -  [" + this.property.userName + "]\n" + "Context - [" + this.property.context + "]\n" + "Provider - [" + this.property.provider + "]");
    }
  }
  
  public void close()
    throws IOException
  {
    try
    {
      this.jmsCommon.closeConnection();
    }
    catch (AdapterException e)
    {
      throw new IOException(e);
    }
  }
  
  public void onMessage(Message msg)
  {
    try
    {
      if ((msg instanceof TextMessage))
      {
        String txt = ((TextMessage)msg).getText();
        if (this.logger.isTraceEnabled()) {
          this.logger.trace("JMS message of type TextMessage containing " + txt.getBytes().length + " has been received");
        }
        ByteBuffer buffer = ByteBuffer.allocate(txt.getBytes().length);
        buffer.put(txt.getBytes());
        buffer.flip();
        
        super.enqueue(buffer);
      }
      else
      {
        this.logger.warn("Received message of type \"" + msg.getJMSType() + "\" is not supported by JMSReader, only TextMessage is supported");
      }
    }
    catch (JMSException exp)
    {
      exp.printStackTrace();
    }
  }
  
  private void enqueue(ByteBuffer buffer)
  {
    int sizeToCopy = 0;
    do
    {
      ByteBuffer tmpBuffer = ByteBuffer.allocate(blockSize());
      sizeToCopy = tmpBuffer.limit();
      if (buffer.limit() - buffer.position() < sizeToCopy) {
        sizeToCopy = buffer.limit() - buffer.position();
      }
      buffer.get(tmpBuffer.array(), 0, sizeToCopy);
      tmpBuffer.limit(sizeToCopy);
      super.enqueue(tmpBuffer);
    } while (buffer.position() < buffer.limit());
  }
}
