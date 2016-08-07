 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class FileBank
   extends Observable
   implements WatcherCallback
 {
   static Logger logger = Logger.getLogger(FileBank.class);
   
 
   public static String INSTANCE = "DirectoryInstance";
   
 
   public static String OBSERVER = "Observer";
   protected Map<String, FileSequencer> sequencerMap;
   protected static Map<String, Pattern> matcherMap = new TreeMap();
   public static AtomicInteger instanceCnt = new AtomicInteger(0);
   Watcher watcher;
   Observer observer;
   Subject subject;
   FileSequencer localSequencer;
   protected Property prop;
   
   public FileBank(Property prop) throws AdapterException {
     this.prop = prop;
     this.sequencerMap = new TreeMap();
     this.observer = ((Observer)prop.getMap().get(OBSERVER));
     this.subject = new Subject();
     try {
       this.watcher = WatcherFactory.createWatcher(prop.propMap, this);
       this.watcher.init();
       
       this.localSequencer = createSequencer(prop);
       this.sequencerMap.put(prop.wildcard, this.localSequencer);
     } catch (IOException e) {
       throw new AdapterException("Got exception while creating directory instance", e);
     }
   }
   
   protected FileSequencer createSequencer(Property prop) throws AdapterException {
     try {
       return FileSequencer.load(prop);
     } catch (AdapterException e) {
       throw new AdapterException("Got exception while creating Sequencer", e);
     }
   }
   
   public static String extractProcessGroup(String pGroupRegEx, File file) {
     Pattern pattern = null;
     if ((pattern = (Pattern)matcherMap.get(pGroupRegEx)) == null) {
       pattern = Pattern.compile(pGroupRegEx);
       matcherMap.put(pGroupRegEx, pattern);
     }
     Matcher matcher = pattern.matcher(file.getName());
     if (matcher.find())
       return "*" + matcher.group() + "*";
     return file.getName();
   }
   
   public void updateInstance(Property prop) throws AdapterException
   {}
   
   public void start() throws AdapterException {
     if (logger.isDebugEnabled())
       logger.debug("Starting directory instance for {" + this.prop.wildcard + "}");
     this.watcher.start();
   }
   
   public void stop(String wildcard)
   {
     if (logger.isDebugEnabled())
       logger.debug("Stopping directory instance for {" + wildcard + "}");
     this.sequencerMap.remove(wildcard);
     instanceCnt.decrementAndGet();
     
     stop();
   }
   
   public void stop() {
     if (this.sequencerMap.isEmpty()) {
       if (logger.isDebugEnabled())
         logger.debug("No active groups, stopping watcher thead");
       this.watcher.stop();
     }
     else if (logger.isDebugEnabled()) {
       logger.debug("No of active groups {" + this.sequencerMap.size() + "} found for {" + this.prop.wildcard + "}");
     }
   }
   
   public boolean isEOFReached(String wildcard, InputStream in, boolean fileChanged) throws IOException
   {
     return this.localSequencer.isEOFReached(in, fileChanged);
   }
   
   public boolean isEmpty(String wildcard) {
     return this.localSequencer.isEmpty();
   }
   
   public FileDetails getNextFile(String wildcard, File file, long position) {
     File fileObj = this.localSequencer.getNextFile(file, position);
     if (fileObj != null) {
       FileDetails fd = new FileDetails();
       fd.file = fileObj;
       fd.startPosition = this.localSequencer.getPosition();
       return fd;
     }
     return null;
   }
   
   public long getPosition(String wildcard) {
     return this.localSequencer.getPosition();
   }
   
   public boolean position(String wildcard, String fileToRecover) {
     return this.localSequencer.position(fileToRecover);
   }
   
 
 
   public void onFileCreate(File file)
   {
     if (logger.isDebugEnabled())
       logger.debug("New file created {" + file.getName() + "}");
     this.localSequencer.onFileCreate(file);
   }
   
   public void onFileDelete(File file)
   {
     if (logger.isDebugEnabled())
       logger.debug("File {" + file.getName() + "} is deleted");
     this.localSequencer.onFileDelete(file);
   }
   
   public void onFileDelete(String fileKey, String fileName)
   {
     if (logger.isDebugEnabled())
       logger.debug("FileKey {" + fileKey + "} {" + fileName + "} is deleted");
     this.localSequencer.onFileDelete(fileKey, fileName);
   }
   
   public void onFileModify(File file)
   {
     if (logger.isDebugEnabled())
       logger.debug("File {" + file.getName() + "} is modified");
     this.localSequencer.onFileModify(file);
   }
   
   public class FileDetails
   {
     public File file;
     public long startPosition;
     
     public FileDetails() {}
   }
   
   public class Subject
   {
     public File file;
     public WatchEvent.Kind<Path> event;
     
     public Subject() {}
   }
 }

