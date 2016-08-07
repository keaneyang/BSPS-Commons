 package com.bloom.source.lib.constant;
 
 

 /*
  * CDC 所定义的常量
  */
 
 public class CDCConstant
   extends Constant
 {
   public static int WA_SIZE_ERROR_BUFFER = 3000;
   public static int WA_SIZE_DBNAME = 256;
   public static int WA_SIZE_USERNAME = 256;
   public static int WA_SIZE_PASSWORD = 256;
   public static int WA_SIZE_TABLENAME = 128;
   public static int WA_SIZE_SCHEMANAME = 256;
   public static int WA_SIZE_COMMON = 256;
   public static int WA_SIZE_TABLELIST = 2048;
   public static int WA_SIZE_LSN = 128;
   public static int WA_SIZE_TIMESTAMP = 8;
   public static int WA_SIZE_COLUMNCOUNT = 2048;
   public static int WA_SIZE_ROWDATA = 65536;
   public static int WA_MESSAGE_BUFFER = 262144;
   
   public static final int PB_RECORD_SIZE_LENGTH = INTEGER_SIZE;
   
   public static final int WA_UNSUPPORTED = 0;
   public static final int WA_BYTE = 1;
   public static final int WA_SIGNED_BYTE = 2;
   public static final int WA_SHORT = 3;
   public static final int WA_SIGNED_SHORT = 4;
   public static final int WA_INTEGER = 5;
   public static final int WA_SIGNED_INTEGER = 6;
   public static final int WA_LONG = 7;
   public static final int WA_SIGNED_LONG = 8;
   public static final int WA_FLOAT = 9;
   public static final int WA_DOUBLE = 10;
   public static final int WA_STRING = 11;
   public static final int WA_UTF16_STRING = 12;
   public static final int WA_BINARY = 13;
   public static final int WA_DATETIME = 14;
   public static final int WA_DATE = 15;
   public static final int WA_BLOB = 16;
   public static final int WA_CLOB = 17;
   public static final int WA_UTF16_CLOB = 18;
   public static int MIN_WAIT_TIME = 1000;
   public static final int MAX_WAIT_TIME = 30720;
   public static String ACTOR_NAME = "READER";
   public static String ACTOR_UID = "23434324";
   public static String DRIVER_TYPE = "CDCREADER";
   
   public static final int MAX_CONNECTION_ATTEMPT = 10;
   public static final int RESPONSE_WAIT_TIME = 20;
   public static final long CONNECTION_WAIT_TIME = 50L;
   
   public static String getCorrespondingClassForCDCType(int cdcType)
   {
     switch (cdcType) {
     case 2: 
       return "java.lang.Byte";
     case 1: 
     case 4: 
       return "java.lang.Short";
     case 3: 
     case 6: 
       return "java.lang.Integer";
     case 5: 
     case 7: 
     case 8: 
       return "java.lang.Long";
     case 9: 
       return "java.lang.Float";
     case 10: 
       return "java.lang.Double";
     case 11: 
     case 12: 
       return "java.lang.String";
     case 15: 
       return "org.joda.time.LocalDate";
     case 14: 
       return "org.joda.time.DateTime";
     case 13: 
     case 16: 
       return "java.lang.Object";
     case 17: 
     case 18: 
       return "java.lang.String";
     case 0: 
       return "java.lang.String";
     }
     
     throw new RuntimeException("Type " + cdcType + " not a valid CDC Data type");
   }
 }

