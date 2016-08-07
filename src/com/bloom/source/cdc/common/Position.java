 package com.bloom.source.cdc.common;
 
 import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.source.lib.intf.IPosition;
import com.bloom.source.lib.type.positiontype;
import com.google.protobuf.ByteString;
import com.webaction.source.cdc.gpb.GPBCommon;
import com.webaction.source.cdc.gpb.GPBCommon._Position;
import com.webaction.source.cdc.gpb.GPBCommon._Position.Builder;
import com.webaction.source.cdc.gpb.GPBCommon._Position.PositionType;
import java.util.Arrays;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class Position
   implements IPosition
 {
   positiontype positionType;
   byte[] positionValue;
   GPBCommon._Position gpbPosition = null;
   GPBCommon._Position.Builder gpbPositionBuilder = null;
   
 
 
 
 
 
 
 
   public Position(positiontype posType, byte[] value)
   {
     this.positionType = posType;
     if (value != null)
       this.positionValue = Arrays.copyOf(value, value.length);
     this.gpbPositionBuilder = GPBCommon._Position.newBuilder();
   }
   
 
 
 
 
 
 
   private GPBCommon._Position.PositionType getGPBPositionType()
   {
     GPBCommon._Position.PositionType posType = null;
     
     switch (this.positionType) {
     case WA_POSITION_LSN: 
       posType = GPBCommon._Position.PositionType.POSITION_LSN;
       break;
     case WA_POSITION_EOF: 
       posType = GPBCommon._Position.PositionType.POSITION_EOF;
       break;
     case WA_POSITION_TIMESTAMP: 
       posType = GPBCommon._Position.PositionType.POSITION_TIMESTAMP;
     }
     
     
     if (posType == null) {
       posType = GPBCommon._Position.PositionType.POSITION_EOF;
     }
     return posType;
   }
   
 
 
 
 
   public GPBCommon._Position getGPBPosition()
   {
     ByteString value = null;
     
     if (this.positionValue != null) {
       this.gpbPositionBuilder.setValue(ByteString.copyFrom(this.positionValue));
     }
     
     this.gpbPositionBuilder.setType(getGPBPositionType());
     
     this.gpbPosition = this.gpbPositionBuilder.build();
     
     return this.gpbPosition;
   }
   
 
   public positiontype getPositionType()
     throws TransactionManagerException
   {
     return this.positionType;
   }
   
 
 
 
   public byte[] getPositionValue()
     throws TransactionManagerException
   {
     return this.positionValue;
   }
 }

