 package com.bloom.source.kafka;
 
 import com.bloom.source.lib.constant.CDCConstant;
import com.bloom.source.lib.meta.ByteColumn;
import com.bloom.source.lib.meta.Column;
import com.bloom.source.lib.meta.IntegerColumn;
import com.bloom.source.lib.meta.LongColumn;
import com.bloom.source.lib.meta.StringColumn;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class OLVPipedInputStream
   extends PipedInputStream
 {
   private static final int DEFAULT_PIPE_LENGHT = 1024;
   private int bytesRemaining = 0;
   
 
 
   private long kafkaMessageOffset = 0L;
   
   private byte[] olvBuffer;
   private int olvBufferOffset = 0;
   
   private Column[] columns;
   private Object[] data;
   private int lastProcessedColumn = 0;
   private boolean readNewMessage = false;
   
 
 
 
   public OLVPipedInputStream()
   {
     initOLVBuffer(1024);
     initColumns();
   }
   
   public OLVPipedInputStream(int pipeLength)
   {
     super(pipeLength);
     initOLVBuffer(pipeLength);
     initColumns();
   }
   
   public OLVPipedInputStream(PipedOutputStream src, int pipeLength) throws IOException {
     super(src, pipeLength);
     initOLVBuffer(pipeLength);
     connect(src);
     initColumns();
   }
   
   public OLVPipedInputStream(PipedOutputStream src) throws IOException {
     super(src);
     initOLVBuffer(1024);
     connect(src);
     initColumns();
   }
   
   public void initOLVBuffer(int pipeLength) {
     if (pipeLength <= 0) {
       throw new IllegalArgumentException("Pipe Size <= 0");
     }
     this.olvBuffer = new byte[pipeLength];
   }
   
   private void initColumns() {
     this.columns = new Column[4];
     this.columns[0] = new LongColumn();
     this.columns[0].setSize(CDCConstant.LONG_SIZE);
     this.columns[1] = new ByteColumn();
     this.columns[1].setSize(CDCConstant.BYTE_SIZE);
     this.columns[2] = new IntegerColumn();
     this.columns[2].setSize(CDCConstant.INTEGER_SIZE);
     this.columns[3] = new StringColumn();
     this.data = new Object[4];
   }
   
 
 
 
   public int read(byte[] b)
     throws IOException
   {
     return 0;
   }
   
   public int available() throws IOException
   {
     return super.available();
   }
   
   public void connect(PipedOutputStream src) throws IOException
   {
     super.connect(src);
   }
   
 
 
 
 
 
 
 
 
 
 
 
   public int read(byte[] b, int offset, int length)
     throws IOException
   {
     int index = this.lastProcessedColumn;
     this.readNewMessage = false;
     if (length > 0) {
       while (index < this.columns.length) {
         if (index != 3)
         {
           int bytes = super.read(this.olvBuffer, this.olvBufferOffset, this.columns[index].getSize() - this.olvBufferOffset);
           
           if (bytes < this.columns[index].getSize() - this.olvBufferOffset) {
             this.olvBufferOffset += bytes;
             this.lastProcessedColumn = index;
 
 
           }
           else
           {
 
             this.olvBufferOffset = 0;
             this.data[index] = this.columns[index].getValue(this.olvBuffer, 0, this.columns[index].getSize());
             this.readNewMessage = true;
             index++;
           }
         } else {
           this.columns[index].setSize(((Integer)this.data[2]).intValue());
           
           if (this.columns[index].getSize() < length)
           {
             int bytes = super.read(b, offset, this.columns[index].getSize());
             if (bytes < this.columns[index].getSize()) {
               this.lastProcessedColumn = index;
               this.data[2] = Integer.valueOf(this.columns[index].getSize() - bytes);
             }
             else {
               this.lastProcessedColumn = 0;
             }
             return bytes;
           }
           
           int bytes = super.read(b, offset, length);
           this.lastProcessedColumn = index;
           this.data[2] = Integer.valueOf(this.columns[index].getSize() - bytes);
           return bytes;
         }
       }
     }
     
 
 
     return 0;
   }
   
 
 
 
 
   public static void main(String[] args) {}
   
 
 
 
   public long getKafkaMessageOffset()
   {
     return ((Long)this.data[0]).longValue();
   }
   
   public boolean isNewMessage()
   {
     if ((this.data[1] != null) && (((Byte)this.data[1]).byteValue() != 1) && (this.readNewMessage)) {
       return true;
     }
     return false;
   }
 }

