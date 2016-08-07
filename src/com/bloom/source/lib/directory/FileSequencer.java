 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Map;
import org.apache.log4j.Logger;
 
 public abstract class FileSequencer
   implements WatcherCallback
 {
   Logger logger = Logger.getLogger(FileSequencer.class);
   
   Map<String, Object> propMap;
   public static String LOG4J_SEQUENCER = "log4j";
   public static String DEFAULT = "Default";
   
 
   protected int retryCnt;
   
   public static final int MAX_RETRY_CNT = 5;
   
 
   public FileSequencer(Map<String, Object> map)
     throws IOException, AdapterException
   {
     this.propMap = map;
   }
   
   public boolean isEmpty() {
     return true;
   }
   
 
   public abstract void onFileCreate(File paramFile);
   
 
   public abstract void onFileDelete(File paramFile);
   
 
   public abstract void onFileModify(File paramFile);
   
   public File getNextFile(File file, long position)
   {
     return null;
   }
   
   public long getPosition() {
     return 0L;
   }
   
   public boolean position(String fileName) {
     return true;
   }
   
 
   protected void postProcess(File file) {}
   
 
   protected void preProcess(File file) {}
   
   protected int fileListSize()
   {
     return 0;
   }
   
 
 
 
   boolean flag = true;
   
   public boolean isEOFReached(InputStream inputStream, boolean fileChanged) throws IOException { if ((fileChanged) || (inputStream.available() <= 0)) {
       if (fileListSize() > 0)
       {
 
 
         if (((fileChanged) || (inputStream.available() <= 0)) && (this.retryCnt < 5)) {
           if ((fileChanged) || (inputStream.available() <= 0)) {
             this.retryCnt += 1;
           } else {
             this.retryCnt = 0;
           }
           
 
 
           return false;
         }
         
 
 
 
         this.retryCnt = 0;
         this.flag = true;
         return true;
       }
       if (fileChanged) {
         this.logger.debug("File has changed, so going to sequencer to get next file");
         return true;
       }
       if (this.flag)
         this.logger.debug("No more file to process...");
       this.flag = false;
     }
     
 
 
 
     return false;
   }
   
   public static FileSequencer load(Property tmpProp) throws AdapterException {
     FileSequencer sequencer = null;
     
     String packageName = "com.bloom.proc.";
     String className = tmpProp.getString(Property.ROLLOVER_STYLE, null);
     sequencer = load(className, tmpProp);
     if (sequencer == null) {
       if (className == null)
         className = Property.DEFAULT_FILE_SEQUENCER;
       try { 
    	   String fullyQualifiedName;
         if (className.contains(".")) {
           fullyQualifiedName = className;
         } else
           fullyQualifiedName = packageName + className;
         Class<?> sequencerClass = null;
         try {
           sequencerClass = Class.forName(fullyQualifiedName);
         } catch (Exception exp) {
           try {
             sequencerClass = Class.forName(className);
           } catch (Exception sExp) {
             throw new AdapterException("Couldn't load {" + className + "} sequencer implementation");
           }
         }
         sequencer = (FileSequencer)sequencerClass.getConstructor(new Class[] { Map.class }).newInstance(new Object[] { tmpProp.getMap() });
       } catch (Exception e) {
         throw new AdapterException("Got exception while instantiating FileSequencer {" + className + "}", e);
       }
     }
     return sequencer;
   }
   
   public static FileSequencer load(String sequncerName, Property tmpProp) throws AdapterException {
     FileSequencer sequencer = null;
     try {
       if (LOG4J_SEQUENCER.equalsIgnoreCase(sequncerName)) {
         sequencer = new RollingFileSequencer(tmpProp.getMap());
       } else if (DEFAULT.equalsIgnoreCase(sequncerName))
         sequencer = new DefaultFileSequencer(tmpProp.getMap());
     } catch (IOException e) {
       throw new AdapterException("Got exception while instantiating FileSequencer {" + sequncerName + "}", e);
     }
     return sequencer;
   }
   
   class FileDetail
   {
     File file;
     long position;
     
     FileDetail() {}
   }
 }

