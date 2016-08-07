package com.bloom.source.lib.prop;

import com.bloom.security.Password;
import com.bloom.source.lib.constant.Constant;

import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public class Property
{
  private Logger logger = Logger.getLogger(Property.class);
  public final String USER_NAME = "UserName";
  public final String PASSWORD = "Password";
  public final String TOPIC = "Topic";
  public final String QUEUE_NAME = "queuename";
  public final String CONTEXT = "Ctx";
  public final String PROVIDER = "Provider";
  public final int MAX_BLOCK_SIZE = 10240;
  public final String ROOT_NODE = "rootnode";
  public static String COMPRESSION_TYPE = "compressiontype";
  public static String ARCHIVE_TYPE = "archiveType";
  public static String CHARSET = "charset";
  public static String READER_TYPE = "readerType";
  public static String IPADDRESS = "ipaddress";
  public final String AUTHENTICATE_CLIENT = "authenticateclient";
  public final String CONNECTION_FACTORY_NAME = "connectionfactoryname";
  public static String DIRECTORY = "directory";
  public static String WILDCARD = "wildcard";
  public static String GROUP_PATTERN = "grouppattern";
  public static String THREAD_POOL_SIZE = "threadpoolsize";
  public static String YIELD_AFTER = "yieldafter";
  public static String NETWORK_FILE_SYSTEM = "networkfilesystem";
  public static String POLL_INTERVAL = "pollinterval";
  public static int DEFAULT_YIELD_AFTER = 20;
  public static String SOURCE_UUID = "sourceUUID";
  public final String MAX_CONCURRENT_CLIENTS = "maxConcurrentClient";
  public static final String AUTH_FILE_LOCATION = "authfilelocation";
  public static final String SKIP_BOM = "skipbom";
  public static final String NESTED_DIRECTORY = "nesteddirectory";
  public final int DEFAULT_MAX_CONCURRENT_CLIENTS = 5;
  public static String MESSAGE_TYPE = "messagetype";
  public final int DEFAILT_THREAD_POOL_SIZE = 20;
  public final String AUTHENTICATION_POLICY = "authenticationpolicy";
  public final String HADOOP_CONFIGURATION_PATH = "hadoopconfigurationpath";
  public static String ROLLOVER_STYLE = "rolloverstyle";
  public static String DEFAULT_FILE_SEQUENCER = "com.bloom.source.lib.directory.DefaultFileSequencer";
  public String directory;
  public String outdirectory;
  public String wildcard;
  public String remoteaddress;
  public String archivedir;
  public String logtype;
  public String columndelimiterlist;
  public String rowdelimiterlist;
  public String charset;
  public String metaColumnList;
  public String[] nvprecorddelimiter;
  public String[] nvpvaluedelimiter;
  public int eofdelay;
  public int maxcolumnoffset;
  public int connectionTimeout;
  public int readTimeout;
  public int blocksize;
  public int portno;
  public int lineoffset;
  public int headerlineno;
  public int retryAttempt;
  public int threadCount;
  public char rowdelimiter;
  public char columndelimiter;
  public char returndelimiter;
  public char quotecharacter;
  public boolean nocolumndelimiter;
  public boolean trimwhitespace;
  public boolean registerRecursively;
  public boolean positionByEOF;
  public boolean skipBOM;
  public boolean authenticateClient;
  public String[] interestedcolumnList;
  public String ipaddress;
  public String userName;
  public String password;
  public String topic;
  public String queueName;
  public String context;
  public String provider;
  public String hadoopUrl;
  public String rollOverPolicy;
  public String keyStoreType;
  public String keyStore;
  public String keyStorePassword;
  public String aliasConfigFile;
  public String authenticationFileLocation;
  public Map<String, Object> propMap;
  public int maxConcurrentClients;
  public boolean dontBlockOnEOF = false;
  public String connectionFactoryName;
  public String messageType;
  public String groupPattern;
  public int threadPoolSize;
  public String authenticationPolicy;
  public String hadoopConfigurationPath;
  
  protected char getChar(Map<String, Object> map, String key)
  {
    char c = '\000';
    
    Object val = map.get(key);
    if ((val instanceof Character)) {
      c = ((Character)val).charValue();
    } else if (((val instanceof String)) && 
      (((String)val).length() > 0)) {
      c = ((String)val).charAt(0);
    }
    return c;
  }
  
  protected int getInt(Map<String, Object> map, String key)
  {
    int c = 0;
    
    Object val = map.get(key);
    if ((val instanceof Number)) {
      c = ((Number)val).intValue();
    } else if ((val instanceof String)) {
      c = Integer.parseInt((String)val);
    }
    return c;
  }
  
  protected boolean getBoolean(Map<String, Object> map, String key)
  {
    boolean bool = false;
    
    Object val = map.get(key);
    if ((val instanceof Boolean)) {
      bool = ((Boolean)val).booleanValue();
    } else if (((val instanceof String)) && (
      (((String)val).equalsIgnoreCase("true")) || (((String)val).equalsIgnoreCase("yes")))) {
      bool = true;
    }
    return bool;
  }
  
  public Property(Map<String, Object> map)
  {
    this.propMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    this.propMap.putAll(map);
    
    Map<String, Object> mp = this.propMap;
    
    Object rowDelimiterValue = null;Object delimiterValue = null;
    
    this.directory = ((String)mp.get(DIRECTORY));
    this.outdirectory = ((String)mp.get("outdir"));
    
    this.charset = getString(CHARSET, null);
    if (mp.get("blocksize") == null)
    {
      this.blocksize = 64;
    }
    else
    {
      this.blocksize = getInt(mp, "blocksize");
      if (this.blocksize < 0) {
        this.blocksize = (-this.blocksize);
      } else if (this.blocksize == 0) {
        this.blocksize = 64;
      }
      if (this.blocksize > 10240) {
        this.logger.warn("Block size {" + this.blocksize * 1024 + "} quite high, it could increase server memory usage");
      }
    }
    if (mp.get("nocolumndelimiter") == null) {
      this.nocolumndelimiter = false;
    } else {
      this.nocolumndelimiter = getBoolean(mp, "nocolumndelimiter");
    }
    if (mp.get("rowdelimiter") == null)
    {
      this.rowdelimiter = '\n';
    }
    else
    {
      rowDelimiterValue = mp.get("rowdelimiter");
      this.rowdelimiter = getChar(mp, "rowdelimiter");
    }
    if (mp.get("maxcolumncount") == null) {
      this.maxcolumnoffset = Constant.MAX_COLUMN_OFFSET_DEFAULT;
    } else {
      this.maxcolumnoffset = getInt(mp, "maxcolumncount");
    }
    if ((mp.get("columndelimiter") == null) && (!this.nocolumndelimiter))
    {
      this.columndelimiter = ',';
    }
    else
    {
      delimiterValue = mp.get("columndelimiter");
      if (this.nocolumndelimiter) {
        this.columndelimiter = '\000';
      } else {
        this.columndelimiter = getChar(mp, "columndelimiter");
      }
    }
    if ((delimiterValue != null) && ((delimiterValue instanceof String))) {
      this.columndelimiterlist = ((String)delimiterValue);
    } else {
      this.columndelimiterlist = String.valueOf(this.columndelimiter);
    }
    if ((rowDelimiterValue != null) && ((rowDelimiterValue instanceof String))) {
      this.rowdelimiterlist = ((String)rowDelimiterValue);
    } else {
      this.rowdelimiterlist = String.valueOf(this.rowdelimiter);
    }
    if (mp.get("quotecharacter") == null) {
      this.quotecharacter = '"';
    } else {
      this.quotecharacter = getChar(mp, "quotecharacter");
    }
    if (mp.get("eofDelay") == null)
    {
      this.eofdelay = 1000;
    }
    else
    {
      this.eofdelay = getInt(mp, "eofDelay");
      if (this.eofdelay < 0) {
        this.eofdelay = (-this.eofdelay);
      }
    }
    if (mp.get("connectionTimeout") == null)
    {
      this.connectionTimeout = 1000;
    }
    else
    {
      this.connectionTimeout = getInt(mp, "connectionTimeout");
      if (this.connectionTimeout < 0) {
        this.connectionTimeout = (-this.connectionTimeout);
      }
    }
    if (mp.get("readTimeout") == null)
    {
      this.readTimeout = 1000;
    }
    else
    {
      this.readTimeout = getInt(mp, "readTimeout");
      if (this.readTimeout < 0) {
        this.readTimeout = (-this.readTimeout);
      }
    }
    if (mp.get("remoteaddress") == null) {
      this.remoteaddress = Constant.REMOTE_ADDRESS_DEFAULT;
    } else {
      this.remoteaddress = ((String)mp.get("remoteaddress"));
    }
    this.ipaddress = ((String)mp.get(IPADDRESS));
    if (this.ipaddress == null) {
      this.ipaddress = "localhost";
    }
    if (mp.get("portno") == null) {
      this.portno = Constant.PORT_NO_DEFAULT;
    } else {
      this.portno = getInt(mp, "portno");
    }
    if (mp.get("LineNumber") != null) {
      this.lineoffset = getInt(mp, "LineNumber");
    } else {
      this.lineoffset = 0;
    }
    this.returndelimiter = '\r';
    if (mp.get("trimwhitespace") != null) {
      this.trimwhitespace = getBoolean(mp, "trimwhitespace");
    } else {
      this.trimwhitespace = false;
    }
    String colList = (String)mp.get("columnlist");
    if (colList != null)
    {
      String delimiter = (String)mp.get("separator");
      if (delimiter == null) {
        delimiter = ",";
      }
      this.interestedcolumnList = colList.split(delimiter);
    }
    if (mp.get(WILDCARD) != null) {
      this.wildcard = ((String)mp.get(WILDCARD));
    }
    if (mp.get("subdirectory") != null) {
      this.registerRecursively = getBoolean(mp, "subdirectory");
    } else {
      this.registerRecursively = false;
    }
    if (mp.get("MetaColumnList") != null) {
      this.metaColumnList = ((String)mp.get("MetaColumnList"));
    }
    getClass();
    if (mp.get("UserName") != null)
    {
      getClass();this.userName = ((String)mp.get("UserName"));
    }
    getClass();
    if (mp.get("Password") != null)
    {
      getClass();this.password = ((String)mp.get("Password"));
    }
    getClass();
    if (mp.get("Topic") != null)
    {
      getClass();this.topic = ((String)mp.get("Topic"));
    }
    getClass();
    if (mp.get("queuename") != null)
    {
      getClass();this.queueName = ((String)mp.get("queuename"));
    }
    getClass();
    if (mp.get("Ctx") != null)
    {
      getClass();this.context = ((String)mp.get("Ctx"));
    }
    getClass();
    if (mp.get("Provider") != null)
    {
      getClass();this.provider = ((String)mp.get("Provider"));
    }
    if (mp.get("positionByEOF") != null) {
      this.positionByEOF = getBoolean(mp, "positionByEOF");
    } else {
      this.positionByEOF = true;
    }
    if (mp.get("retryAttempt") != null) {
      this.retryAttempt = getInt(mp, "retryAttempt");
    } else {
      this.retryAttempt = -1;
    }
    if (mp.get("hadoopurl") != null)
    {
      this.hadoopUrl = ((String)mp.get("hadoopurl"));
      if (!this.hadoopUrl.endsWith("/")) {
        this.hadoopUrl += "/";
      }
    }
    if (mp.get("skipbom") != null) {
      this.skipBOM = getBoolean(mp, "skipbom");
    } else {
      this.skipBOM = true;
    }
    if (mp.get("rolloverpolicy") != null) {
      this.rollOverPolicy = ((String)mp.get("rolloverpolicy"));
    }
    if (mp.get("threadcount") != null) {
      this.threadCount = getInt(mp, "threadcount");
    }
    if (mp.get("keystoretype") != null) {
      this.keyStoreType = ((String)mp.get("keystoretype"));
    }
    if (mp.get("keystore") != null) {
      this.keyStore = ((String)mp.get("keystore"));
    }
    if (mp.get("keystorepassword") != null) {
      this.keyStorePassword = ((Password)mp.get("keystorepassword")).getPlain();
    }
    this.authenticateClient = getBoolean("authenticateclient", false);
    if (mp.get("alias") != null) {
      this.aliasConfigFile = ((String)mp.get("alias"));
    }
    this.maxConcurrentClients = getInt("maxConcurrentClient", 5);
    this.authenticationFileLocation = getString("authfilelocation", "");
    
    this.dontBlockOnEOF = getBoolean("dontBlockOnEOF", false);
    
    this.connectionFactoryName = getString("connectionfactoryname", null);
    
    this.messageType = getString(MESSAGE_TYPE, "TextMessage");
    
    this.threadPoolSize = getInt(THREAD_POOL_SIZE, 20);
    this.groupPattern = getString(GROUP_PATTERN, null);
    this.authenticationPolicy = getString("authenticationpolicy", null);
    this.hadoopConfigurationPath = getString("hadoopconfigurationpath", null);
  }
  
  public String getString(String key, String defaultValue)
  {
    Object obj = null;
    if ((this.propMap != null) && ((obj = this.propMap.get(key)) != null)) {
      return (String)obj;
    }
    return defaultValue;
  }
  
  public boolean getBoolean(String key, boolean defaultValue)
  {
    boolean bool = defaultValue;
    Object obj;
    if ((this.propMap != null) && ((obj = this.propMap.get(key)) != null)) {
      if ((obj instanceof Boolean)) {
        bool = ((Boolean)obj).booleanValue();
      } else if (((obj instanceof String)) && (
        (((String)obj).equalsIgnoreCase("true")) || (((String)obj).equalsIgnoreCase("yes")))) {
        bool = true;
      }
    }
    return bool;
  }
  
  public char getChar(String key, char defaultValue)
  {
    char value = defaultValue;
    Object obj;
    if ((this.propMap != null) && ((obj = this.propMap.get(key)) != null)) {
      value = ((Character)obj).charValue();
    }
    return value;
  }
  
  public Object getObject(String key, Object defaultValie)
  {
    Object obj = null;
    if (this.propMap != null) {
      obj = this.propMap.get(key);
    }
    return obj;
  }
  
  public int getInt(String key, int defaultValue)
  {
    int value = defaultValue;
    Object obj;
    if ((this.propMap != null) && ((obj = this.propMap.get(key)) != null)) {
      try
      {
        value = ((Integer)obj).intValue();
      }
      catch (ClassCastException ce)
      {
        try
        {
          value = Integer.parseInt(obj.toString());
        }
        catch (Exception e)
        {
          value = defaultValue;
        }
      }
    }
    return value;
  }
  
  public Map<String, Object> getMap()
  {
    return this.propMap;
  }
}
