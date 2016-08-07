 package com.bloom.source.cdc;
 
 import com.bloom.source.lib.intf.Notify;
import com.bloom.source.lib.socket.SocketReader;

import java.nio.ByteBuffer;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class CDCListenerInteractor
   extends SocketReader
 {
   ByteBuffer byteBuffer = null;
   Logger logger = Logger.getLogger(CDCListenerInteractor.class);
   
 
 
 
 
 
 
   public CDCListenerInteractor(Notify rs, CDCProperty prop)
   {
     super(rs, prop);
   }
 }

