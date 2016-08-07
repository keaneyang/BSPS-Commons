 package com.bloom.source.avro.codec;
 
 import java.io.ByteArrayOutputStream;
 import java.io.IOException;
 import java.io.OutputStream;
 import java.nio.ByteBuffer;
 import java.util.zip.Deflater;
 import java.util.zip.DeflaterOutputStream;
 import java.util.zip.Inflater;
 import java.util.zip.InflaterOutputStream;
 import org.apache.avro.file.Codec;
 
 
 
 
 
 
 
 
 public class DeflateCodec
   extends Codec
 {
   private ByteArrayOutputStream outputBuffer;
   private Deflater deflater;
   private Inflater inflater;
   private boolean nowrap = true;
   private int compressionLevel;
   
   public DeflateCodec(int compressionLevel) {
     this.compressionLevel = compressionLevel;
   }
   
   public DeflateCodec() { this.compressionLevel = -1; }
   
 
   public String getName()
   {
     return "deflate";
   }
   
   public ByteBuffer compress(ByteBuffer data) throws IOException
   {
     ByteArrayOutputStream baos = getOutputBuffer(data.remaining());
     DeflaterOutputStream ios = new DeflaterOutputStream(baos, getDeflater());
     writeAndClose(data, ios);
     ByteBuffer result = ByteBuffer.wrap(baos.toByteArray());
     return result;
   }
   
   public ByteBuffer decompress(ByteBuffer data) throws IOException
   {
     ByteArrayOutputStream baos = getOutputBuffer(data.remaining());
     InflaterOutputStream ios = new InflaterOutputStream(baos, getInflater());
     writeAndClose(data, ios);
     ByteBuffer result = ByteBuffer.wrap(baos.toByteArray());
     return result;
   }
   
   private void writeAndClose(ByteBuffer data, OutputStream to) throws IOException {
     byte[] input = data.array();
     int offset = data.arrayOffset() + data.position();
     int length = data.remaining();
     try {
       to.write(input, offset, length);
     } finally {
       to.close();
     }
   }
   
   private Inflater getInflater()
   {
     if (null == this.inflater) {
       this.inflater = new Inflater(this.nowrap);
     }
     this.inflater.reset();
     return this.inflater;
   }
   
   private Deflater getDeflater()
   {
     if (null == this.deflater) {
       this.deflater = new Deflater(this.compressionLevel, this.nowrap);
     }
     this.deflater.reset();
     return this.deflater;
   }
   
   private ByteArrayOutputStream getOutputBuffer(int suggestedLength)
   {
     if (null == this.outputBuffer) {
       this.outputBuffer = new ByteArrayOutputStream(suggestedLength);
     }
     this.outputBuffer.reset();
     return this.outputBuffer;
   }
   
   public int hashCode()
   {
     return this.nowrap ? 0 : 1;
   }
   
   public boolean equals(Object obj)
   {
     if (this == obj)
       return true;
     if (getClass() != obj.getClass())
       return false;
     DeflateCodec other = (DeflateCodec)obj;
     return this.nowrap == other.nowrap;
   }
   
   public String toString()
   {
     return getName() + "-" + this.compressionLevel;
   }
 }

