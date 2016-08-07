 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;
 
 public class NFSWatcher extends Watcher
 {
   Logger logger = Logger.getLogger(NFSWatcher.class);
   public static int DEFAULT_MAX_FILE_CNT = 10000;
   PriorityBlockingQueue<File> fileList;
   private Path directory;
   int poolInterval;
   Map<String, File> keyToFileObjMap;
   Map<String, BasicFileAttributes> keytoFileAttrMap;
   Map<String, Long> creationTimeMap;
   Map<String, String> ignoreKeyMap;
   boolean positionByEOF;
   
   public NFSWatcher(Property prop, WatcherCallback callback) throws IOException
   {
     super(prop, callback);
     this.maxFilesToFetch = 10000;
   }
   
   protected void init() throws AdapterException {
     this.poolInterval = this.prop.getInt(Property.POLL_INTERVAL, 15);
     this.keyToFileObjMap = new HashMap();
     this.keytoFileAttrMap = new HashMap();
     this.creationTimeMap = new HashMap();
     this.fileList = new PriorityBlockingQueue(10, new com.bloom.source.lib.utils.DefaultFileComparator());
     this.ignoreKeyMap = new HashMap();
   }
   
   public void stop()
   {
     if (this.logger.isDebugEnabled())
       this.logger.debug("Stopping NFSWatcher....");
     super.stop();
   }
   
   protected File[] getFiles() throws AdapterException {
     File[] tmpList = super.getFiles();
     for (int itr = 0; itr < tmpList.length; itr++) {
       try {
         BasicFileAttributes attr = Files.readAttributes(tmpList[itr].toPath(), BasicFileAttributes.class, new LinkOption[0]);
         String fileKey = attr.fileKey().toString();
         this.keyToFileObjMap.put(fileKey, tmpList[itr]);
         if (this.prop.positionByEOF) {
           this.ignoreKeyMap.put(fileKey, fileKey);
           this.keytoFileAttrMap.put(fileKey, attr);
         }
       }
       catch (IOException e) {
         e.printStackTrace();
       }
     }
     return tmpList;
   }
   
   public void run() {
     this.directory = Paths.get(this.prop.directory, new String[0]);
     
 
 
 
     int sleepTime = this.poolInterval * 100;
     Map<String, File> deletedFileMap = new java.util.TreeMap();
     String topFileName = null;
     if (this.logger.isDebugEnabled())
       this.logger.debug("Will Ignore the first event for the following file keys {" + this.ignoreKeyMap.toString() + "}");
     while (!this.stopCalled) {
       try {
         int fileCnt = 0;
         boolean foundMatchedFile = false;
         DirectoryStream<Path> listOfPaths = Files.newDirectoryStream(this.directory, new Watcher.WildcardFilter( this.prop.wildcard));
         this.fileList.clear();
         deletedFileMap.clear();
         deletedFileMap.putAll(this.keyToFileObjMap);
         for (Path path : listOfPaths) {
           foundMatchedFile = true;
           File file = path.toFile();
           this.fileList.add(file);
         }
         listOfPaths.close();
         File file; while ((file = (File)this.fileList.poll()) != null) {
           BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class, new LinkOption[0]);
           String fileKey = attr.fileKey().toString();
           File oldFile = (File)this.keyToFileObjMap.get(fileKey);
           Long creationTime = Long.valueOf(attr.creationTime().toMillis());
           if (oldFile == null)
           {
             if (this.logger.isDebugEnabled())
               this.logger.debug("New file {" + file.getName() + "} Key {" + fileKey + "} is created");
             onFileCreate(file);
           }
           else
           {
             BasicFileAttributes oldAttr = (BasicFileAttributes)this.keytoFileAttrMap.get(fileKey);
             if ((oldAttr != null) && 
               (topFileName == null) && (attr.size() > oldAttr.size()))
             {
 
 
               if (this.logger.isDebugEnabled())
                 this.logger.debug("File {" + file.getName() + "} is modified");
               onFileModify(file);
               this.keytoFileAttrMap.remove(fileKey);
               this.ignoreKeyMap.remove(fileKey);
               topFileName = file.getName();
             }
             
 
 
 
             if (!oldFile.getName().equals(file.getName())) {
               boolean ignore = false;
               if ((!this.ignoreKeyMap.isEmpty()) && (this.ignoreKeyMap.get(fileKey) != null)) {
                 ignore = true;
               }
               if (file.getName().equals(topFileName)) {
                 if (this.logger.isDebugEnabled())
                   this.logger.debug("File {" + oldFile.getName() + "} is deleted and its key is reused");
                 onFileDelete(fileKey, file.getName());
                 onFileCreate(file);
                 this.ignoreKeyMap.remove(fileKey);
                 if ((!this.ignoreKeyMap.isEmpty()) && 
                   (this.logger.isDebugEnabled())) {
                   this.logger.debug("Entries to Ignore {" + this.ignoreKeyMap.toString() + "}");
                 }
                 
               }
               else if (ignore) {
                 if (this.logger.isDebugEnabled())
                   this.logger.debug("Ignoring {" + file.getName() + "} Key {" + fileKey + "} event");
               } else {
                 if (this.logger.isDebugEnabled())
                   this.logger.debug("File {" + oldFile.getName() + "} is renamed/moved as {" + file.getName() + "}");
                 onFileCreate(file);
                 onFileDelete(fileKey, file.getName());
               }
             }
           }
           
           if (this.logger.isDebugEnabled())
             this.logger.debug("Key {" + fileKey + "} File {" + file.getName() + "}");
           this.creationTimeMap.put(file.getName(), creationTime);
           this.keyToFileObjMap.put(fileKey, file);
           deletedFileMap.remove(fileKey);
           this.keytoFileAttrMap.put(fileKey, attr);
           fileCnt++;
         }
         if (this.logger.isDebugEnabled()) {
           this.logger.debug("Fetched {" + fileCnt + "} files from {" + this.prop.directory + "}");
         }
         if (!foundMatchedFile)
         {
 
 
 
           this.keyToFileObjMap.clear();
           this.keytoFileAttrMap.clear();
           this.creationTimeMap.clear();
         }
         
 
 
         for (Map.Entry<String, File> entry : deletedFileMap.entrySet()) {
           File obj = (File)entry.getValue();
           String ignoreFk = (String)this.ignoreKeyMap.get(entry.getKey());
           if (ignoreFk != null) {
             this.ignoreKeyMap.remove(ignoreFk);
             if (this.logger.isDebugEnabled())
               this.logger.debug("Assumed the file refers to {" + ignoreFk + "}, is deleted so from IgnoreKey list");
           } else {
             onFileDelete((String)entry.getKey(), obj.getName());
           }
           this.creationTimeMap.remove(obj.getName());
           this.keyToFileObjMap.remove(entry.getKey());
           this.keytoFileAttrMap.remove(entry.getKey());
         }
       } catch (IOException e) {
         this.logger.warn("Got exception while polling {" + this.prop.directory + "}");
       }
       try {
         Thread.sleep(sleepTime);
       }
       catch (InterruptedException e) {}
     }
     if (this.logger.isDebugEnabled()) {
       this.logger.debug("Exiting NFSWatcher thread");
     }
   }
 }

