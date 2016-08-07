 package com.bloom.source.avro.codec;
 
 import java.io.IOException;
 import java.nio.ByteBuffer;
 import java.util.zip.CRC32;
 import org.apache.avro.file.Codec;
 import org.xerial.snappy.Snappy;
 
 
 
 /*
  * snappy 数据压缩编解码
  */
 
 public class SnappyCodec
   extends Codec
 {
   private CRC32 crc32 = new CRC32();
   
   public String getName()
   {
     return "snappy";
   }
   
   public ByteBuffer compress(ByteBuffer in) throws IOException {
     ByteBuffer out = ByteBuffer.allocate(Snappy.maxCompressedLength(in.remaining()) + 4);
     
     int size = Snappy.compress(in.array(), in.position(), in.remaining(), out.array(), 0);
     
     this.crc32.reset();
     this.crc32.update(in.array(), in.position(), in.remaining());
     out.putInt(size, (int)this.crc32.getValue());
     
     out.limit(size + 4);
     
     return out;
   }
   
   public ByteBuffer decompress(ByteBuffer in) throws IOException
   {
     ByteBuffer out = ByteBuffer.allocate(Snappy.uncompressedLength(in.array(), in.position(), in.remaining() - 4));
     
     int size = Snappy.uncompress(in.array(), in.position(), in.remaining() - 4, out.array(), 0);
     
     out.limit(size);
     
     this.crc32.reset();
     this.crc32.update(out.array(), 0, size);
     if (in.getInt(in.limit() - 4) != (int)this.crc32.getValue()) {
       throw new IOException("Checksum failure");
     }
     return out;
   }
   
   public int hashCode() { return getName().hashCode(); }
   
   public boolean equals(Object obj)
   {
     if (this == obj)
       return true;
     if (getClass() != obj.getClass())
       return false;
     return true;
   }
 }

