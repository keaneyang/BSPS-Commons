package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderMalfunctionError;
import java.nio.charset.CoderResult;
import org.apache.log4j.Logger;

public class XMLPositioner
  extends ReaderBase
{
  ByteBuffer buffer;
  CharBuffer internalCharbuffer;
  String rootNode;
  int totalCharSkipped;
  long bytesSkipped;
  boolean hasPositioned;
  CharsetDecoder decoder = null;
  String startTag = null;
  String charSet = null;
  String sourceName = "";
  long startPosition = 0L;
  private Logger logger = Logger.getLogger(XMLPositioner.class);
  
  public XMLPositioner(ReaderBase link)
    throws AdapterException
  {
    super(link);
    
    property().getClass();this.rootNode = property().getString("rootnode", "/");
    this.totalCharSkipped = 0;
    this.hasPositioned = false;
    init(false);
  }
  
  public long skip(long position)
  {
    try
    {
      this.buffer = positionTheReader(position, true, false);
    }
    catch (AdapterException e)
    {
      e.printStackTrace();
    }
    return position;
  }
  
  public long startPosition()
  {
    return this.startPosition;
  }
  
  public long beginOffset()
  {
    return this.totalCharSkipped;
  }
  
  public CheckpointDetail getCheckpointDetail()
  {
    return this.recoveryCheckpoint;
  }
  
  public void position(CheckpointDetail checkpoint, boolean positionFlag)
    throws AdapterException
  {
    this.linkedStrategy.position(checkpoint, false);
    if ((checkpoint != null) && (checkpoint.getRecordEndOffset() != null))
    {
      this.startPosition = checkpoint.seekPosition().longValue();
      
      positionTheReader(0L, false, true);
      
      skipBytes(checkpoint.seekPosition().longValue());
      this.recoveryCheckpoint.seekPosition(checkpoint.seekPosition().longValue());
      this.buffer = positionTheReader(checkpoint.getRecordEndOffset().longValue(), true, false);
      if (this.buffer != null)
      {
        this.recoveryCheckpoint.setSourceName(checkpoint.getSourceName());
        this.recoveryCheckpoint.setRecordBeginOffset(this.recoveryCheckpoint.getRecordBeginOffset().longValue());
        this.recoveryCheckpoint.setRecordEndOffset(0L);
        return;
      }
      return;
    }
    if ((checkpoint != null) || (this.linkedStrategy.property().positionByEOF == true))
    {
      long bytesToSkip;
      if (checkpoint != null) {
        bytesToSkip = checkpoint.seekPosition().longValue();
      } else {
        bytesToSkip = this.linkedStrategy.getEOFPosition();
      }
      positionTheReader(0L, false, true);
      skipBytes(bytesToSkip);
      this.startPosition = bytesToSkip;
      this.recoveryCheckpoint.seekPosition(bytesToSkip);
      this.buffer = positionTheReader(0L, true, false);
    }
    else if (this.logger.isDebugEnabled())
    {
      this.logger.debug("XMLPositioner::position() : Starting from BEGIN posiotion");
    }
  }
  
  public Object readBlock()
    throws AdapterException
  {
    if (this.hasPositioned != true)
    {
      this.buffer = positionTheReader(0L, true, false);
      return null;
    }
    if ((this.buffer != null) && (this.buffer.hasRemaining())) {
      return this.buffer;
    }
    return this.linkedStrategy.readBlock();
  }
  
  public void close()
    throws IOException
  {
    super.close();
  }
  
  private String getCharset(ByteBuffer buffer)
  {
    if (buffer != null)
    {
      String buf = new String(buffer.array(), Charset.forName("UTF-8"));
      int offset = buf.indexOf(">");
      if (offset != -1)
      {
        int charSetOffset = buf.indexOf("encoding");
        if (charSetOffset != -1)
        {
          int endOfCharSet = buf.indexOf(' ', charSetOffset);
          if (endOfCharSet != -1)
          {
            int beginOfCharSet = buf.lastIndexOf('=', endOfCharSet);
            if (beginOfCharSet != -1)
            {
              String charSet = buf.substring(beginOfCharSet + 1, endOfCharSet);
              return charSet;
            }
          }
        }
      }
      else
      {
        return "";
      }
    }
    return "UTF-8";
  }
  
  private String extractHeader(ByteBuffer buff)
  {
    if (buff != null)
    {
      String buf = new String(buff.array(), Charset.forName("UTF-8"));
      int offset = buf.indexOf(">");
      if (offset != -1)
      {
        String header = "";
        if (buf.indexOf("xml") != -1) {
          header = buf.substring(0, offset + 1);
        }
        return header;
      }
    }
    return "";
  }
  
  private String formHeader(String rootNode, String header, String charSet)
  {
    String xmlHeader = header;
    
    String[] nodeList = rootNode.split("/");
    for (int idx = 0; idx < nodeList.length - 1; idx++) {
      if (nodeList[idx].length() != 0) {
        xmlHeader = xmlHeader + "<" + nodeList[idx] + ">\n";
      }
    }
    return xmlHeader;
  }
  
  private ByteBuffer formInitialBuffer(String header, CharBuffer leftOutData, String charSet)
    throws AdapterException
  {
    String xmlString = "";
    xmlString = xmlString + header;
    Charset charset = Charset.forName(charSet);
    CharsetEncoder encoder = charset.newEncoder();
    ByteBuffer byteBuff = null;
    
    xmlString = xmlString + leftOutData.toString();
    CharBuffer buffer = CharBuffer.allocate(xmlString.length());
    buffer.put(xmlString);
    buffer.flip();
    try
    {
      byteBuff = encoder.encode(buffer);
    }
    catch (CharacterCodingException e)
    {
      throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME, e);
    }
    return byteBuff;
  }
  
  private ByteBuffer positionTheReader(long charOffset, boolean isCharOffset, boolean initalizeHeader)
    throws AdapterException
  {
    long charToSkip = charOffset;
    ByteBuffer tmpBuffer = ByteBuffer.allocate(blockSize() * 2);
    String[] nodeList = this.rootNode.split("/");
    
    String startTagOfLeafe = "<" + nodeList[(nodeList.length - 1)] + ">";
    String startTagOfLeafeOtherForm = "<" + nodeList[(nodeList.length - 1)] + " ";
    if (this.sourceName.length() == 0) {
      this.sourceName = name();
    }
    while ((this.stopRead != true) && (this.hasPositioned != true)) {
      try
      {
        ByteBuffer tmpBuf = (ByteBuffer)this.linkedStrategy.readBlock();
        if (tmpBuf == null) {
          return null;
        }
        if (this.sourceName.equals(name()) != true)
        {
          this.recoveryCheckpoint.setSourceName(name());
          this.recoveryCheckpoint.seekPosition(0L);
          this.sourceName = name();
        }
        tmpBuffer.clear();
        tmpBuffer.flip();
        System.arraycopy(tmpBuf.array(), 0, tmpBuffer.array(), tmpBuffer.limit(), tmpBuf.limit());
        tmpBuffer.limit(tmpBuffer.limit() + tmpBuf.limit());
        if (this.decoder == null)
        {
          this.charSet = getCharset(tmpBuffer);
          if ((this.charSet.length() == 0) || ((isCharOffset != true) && (charToSkip <= tmpBuf.limit()) && (initalizeHeader != true)))
          {
            this.bytesSkipped = 0L;
            this.hasPositioned = true;
            return tmpBuffer;
          }
          this.startTag = extractHeader(tmpBuffer);
          this.decoder = Charset.forName(this.charSet).newDecoder();
          if (initalizeHeader) {
            return null;
          }
          this.bytesSkipped = charToSkip;
          this.startPosition = charToSkip;
        }
        CharBuffer charBuff = CharBuffer.allocate(blockSize());
        charBuff.clear();
        CoderResult result = this.decoder.decode(tmpBuffer, charBuff, false);
        if (result.isError()) {
          throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME);
        }
        charBuff.flip();
        if ((isCharOffset == true) && (charToSkip > charBuff.limit()))
        {
          charToSkip -= charBuff.limit();
          this.totalCharSkipped += charBuff.limit();
        }
        else if ((!isCharOffset) && (charToSkip > tmpBuffer.limit()))
        {
          charToSkip -= tmpBuffer.limit();
          this.totalCharSkipped += charBuff.limit();
        }
        else
        {
          if (!isCharOffset)
          {
            if (this.totalCharSkipped == 0) {
              charBuff.position(this.totalCharSkipped);
            }
          }
          else if (charToSkip > 0L)
          {
            charBuff.position((int)charToSkip);
            charToSkip = 0L;
          }
          else
          {
            charBuff.position((int)charToSkip);
          }
          String tmpStr = charBuff.toString();
          int startTagOff = tmpStr.indexOf(startTagOfLeafe);
          if (startTagOff == -1) {
            startTagOff = tmpStr.indexOf(startTagOfLeafeOtherForm);
          }
          if (startTagOff == -1)
          {
            this.totalCharSkipped += charBuff.limit();
            
            tmpBuffer.clear();
            if (charToSkip == 0L) {
              return null;
            }
          }
          else
          {
            int tmp = startTagOff;
            charBuff.position(charBuff.position() + tmp);
            String xmlHeader = formHeader(this.rootNode, this.startTag, this.charSet);
            if (charToSkip == 0L)
            {
              this.totalCharSkipped += charBuff.position();
              this.recoveryCheckpoint.setRecordBeginOffset(this.totalCharSkipped);
            }
            this.totalCharSkipped = xmlHeader.length();
            this.recoveryCheckpoint.setRecordLength(this.totalCharSkipped);
            
            this.hasPositioned = true;
            return formInitialBuffer(xmlHeader, charBuff, this.charSet);
          }
        }
      }
      catch (CoderMalfunctionError exp)
      {
        throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME);
      }
    }
    return null;
  }
}
