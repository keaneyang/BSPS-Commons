 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.utils.DefaultFileComparator;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;
 
 public class RollingFileSequencer extends FileSequencer
 {
   Logger logger = Logger.getLogger(RollingFileSequencer.class);
   
   PriorityBlockingQueue<File> fileList;
   
   Map<String, File> keyToFileObjMap;
   Map<String, String> nameToKeyMap;
   Map<String, Integer> keyCreateCntMap;
   Map<String, Long> creationTimeMap;
   String lastAddedFile;
   long positionToStart;
   String lastKey;
   File lastFileObj;
   String ignoreDeleteFor;
   private static int DELETED_FILE_RETRY = 5;
   
 
 
 
 
 
 
 
 
 
 
 
 
   int deletedFileRetry;
   
 
 
 
 
 
 
 
 
 
 
 
 
 
   public RollingFileSequencer(Map<String, Object> map)
     throws IOException, AdapterException
   {
     super(map);
     init();
   }
   
   private void init() {
     this.fileList = new PriorityBlockingQueue(10, new DefaultFileComparator());
     this.keyToFileObjMap = new TreeMap();
     this.nameToKeyMap = new TreeMap();
     this.keyCreateCntMap = new TreeMap();
     this.creationTimeMap = new TreeMap();
   }
   
   public void onFileCreate(File file)
   {
     addFile(file, true);
   }
   
   public boolean isEmpty() {
     return (this.fileList.size() == 0) && (this.keyToFileObjMap.isEmpty());
   }
   
   public void onFileDelete(String fileKey, String fileName)
   {
     Integer refCnt = (Integer)this.keyCreateCntMap.get(fileKey);
     File fileObj = (File)this.keyToFileObjMap.get(fileKey);
     if (fileObj != null) {
       if (refCnt != null) {
         Integer localInteger1 = refCnt;Integer localInteger2 = refCnt = Integer.valueOf(refCnt.intValue() - 1);
         if (refCnt.intValue() == 0) {
           if (this.fileList.contains(fileObj)) {
             if (this.logger.isDebugEnabled())
               this.logger.debug("Zero reference filekey file {" + fileObj.getName() + "} Key {" + fileKey + "}, removing from file list. No of files to process {" + this.fileList.size() + "}");
             this.fileList.remove(fileObj);
           }
           else if (this.logger.isDebugEnabled()) {
             this.logger.debug("Zero reference filekey file {" + fileObj.getName() + "} Key {" + fileKey + "}, deleting filekey reference");
           }
           this.keyToFileObjMap.remove(fileKey);
           this.keyCreateCntMap.remove(fileKey);
         } else {
           this.keyCreateCntMap.put(fileKey, refCnt);
           if (this.logger.isDebugEnabled())
             this.logger.debug("Reduced the reference count of {" + fileObj.getName() + "} Key {" + fileKey + "} to {" + refCnt + "}");
         }
       } else {
         this.logger.debug("FileKey reference count can not be NULL, there is some logical issue");
       }
     } else {
       this.logger.debug("File is not seen before, ignoring the delete event");
     }
   }
   
   public void onFileDelete(File file)
   {
     synchronized (this.fileList) {
       String fileName = file.getName();
       String fileKey = (String)this.nameToKeyMap.get(fileName);
       if (this.logger.isDebugEnabled())
         this.logger.debug("Got Delete event for {" + fileName + "} Key {" + fileKey + "}");
       if (fileKey == null) {
         if (this.logger.isDebugEnabled())
           this.logger.debug("File {" + fileName + "} has no associated key in cache, ignoring delete event");
         return;
       }
       Integer refCnt = (Integer)this.keyCreateCntMap.get(fileKey);
       File fileObj = (File)this.keyToFileObjMap.get(fileKey);
       if (fileObj != null) {
         if (refCnt != null) {
           Integer localInteger1 = refCnt;Integer localInteger2 = refCnt = Integer.valueOf(refCnt.intValue() - 1);
           if (refCnt.intValue() == 0) {
             if (this.fileList.contains(fileObj)) {
               if (this.logger.isDebugEnabled())
                 this.logger.debug("Zero reference filekey file {" + file.getName() + "} Key {" + fileKey + "}, removing from file list. No of files to process {" + this.fileList.size() + "}");
               this.fileList.remove(fileObj);
             }
             else if (this.logger.isDebugEnabled()) {
               this.logger.debug("Zero reference filekey file {" + file.getName() + "} Key {" + fileKey + "}, deleting filekey reference");
             }
             this.keyToFileObjMap.remove(fileKey);
             this.keyCreateCntMap.remove(fileKey);
           } else {
             this.keyCreateCntMap.put(fileKey, refCnt);
             if (this.logger.isDebugEnabled())
               this.logger.debug("Reduced the reference count of {" + fileName + "} Key {" + fileKey + "} to {" + refCnt + "}");
           }
         } else {
           this.logger.debug("FileKey reference count can not be NULL, there is some logical issue");
         }
       } else {
         this.logger.debug("File is not seen before, ignoring the delete event");
       }
     }
     if (this.logger.isDebugEnabled()) {
       this.logger.debug("End of onFileDelete()");
     }
   }
   
   public void onFileModify(File file) {
     addFile(file, false);
   }
   
   private void addFile(File file, boolean isCreateEvent) {
     synchronized (this.fileList) {
       try {
         BasicFileAttributes attr = Files.readAttributes(file.toPath(), BasicFileAttributes.class, new LinkOption[0]);
         String fileKey = attr.fileKey().toString();
         String fileName = file.getName();
         File oldFileObject = (File)this.keyToFileObjMap.get(fileKey);
         Long creationTime = Long.valueOf(attr.creationTime().toMillis());
         if (this.logger.isDebugEnabled()) {
           this.logger.debug("Got event for {" + fileKey + "} {" + fileName + "} Create {" + attr.creationTime().toMillis() + "} Lastmod {" + attr.lastModifiedTime().toMillis() + "}");
         }
         Integer cnt = (Integer)this.keyCreateCntMap.get(fileKey);
         if (cnt == null) {
           this.keyCreateCntMap.put(fileKey, Integer.valueOf(1));
         } else {
           this.keyCreateCntMap.put(fileKey, Integer.valueOf(cnt.intValue() + 1));
         }
         this.ignoreDeleteFor = fileKey;
         if (oldFileObject == null) {
           this.fileList.add(file);
           if (this.logger.isDebugEnabled()) {
             this.logger.debug("Seen new file entry {" + fileName + "} {" + fileKey + "} adding into processing list. No of Files to process {" + this.fileList.size() + "}");
           }
         } else if ((this.lastKey != null) && (this.lastKey.equals(fileKey))) {
           if (this.logger.isDebugEnabled()) {
             this.logger.debug("Processed file is getting renamed from {" + oldFileObject.getName() + "} to {" + file.getName() + "}");
           }
         } else if (this.fileList.contains(oldFileObject)) {
           this.fileList.remove(oldFileObject);
           this.fileList.add(file);
           if (this.logger.isDebugEnabled()) {
             this.logger.debug("Removed {" + oldFileObject.getName() + "} and added {" + file.getName() + "} . No of files to process {" + this.fileList.size() + "}");
           }
           
         }
         else if (this.logger.isDebugEnabled()) {
           this.logger.debug("File {" + fileName + "} Key {" + fileKey + "} is already processed");
         }
         
 
 
         this.nameToKeyMap.put(fileName, fileKey);
         this.creationTimeMap.put(fileName, creationTime);
         this.keyToFileObjMap.put(fileKey, file);
       } catch (IOException e) {
         this.logger.warn("Got exception while getting file attribute", e);
       }
       if (this.logger.isDebugEnabled()) {
         this.logger.debug("Files to process {" + this.fileList.size() + "}");
       }
     }
   }
   
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
   public File getNextFile(File file, long position)
   {
     File newFile = null;
     BasicFileAttributes attr = null;
     synchronized (this.fileList) {
       this.positionToStart = 0L;
       if (file == null) {
         newFile = (File)this.fileList.poll();
       }
       else if (this.lastKey == null) {
         if (this.logger.isDebugEnabled())
           this.logger.warn("LastKey is null, which is not suppose to be");
         if ((this.lastFileObj == null) && 
           (this.logger.isDebugEnabled())) {
           this.logger.debug("Last file {" + this.lastFileObj.getName() + "}");
         }
       } else {
         newFile = (File)this.keyToFileObjMap.get(this.lastKey);
         if (newFile == null) {
           if (this.logger.isDebugEnabled())
             this.logger.debug("File key {" + this.lastKey + "} is not found in cache, moving to next file");
           newFile = (File)this.fileList.poll();
         } else {
           try {
             attr = Files.readAttributes(newFile.toPath(), BasicFileAttributes.class, new LinkOption[0]);
           } catch (IOException e) {
             if (this.deletedFileRetry++ > DELETED_FILE_RETRY) {
               if (this.logger.isDebugEnabled())
                 this.logger.debug("Waited for {" + this.deletedFileRetry + "} iteration, assuming the file is permanently deleted from the system and proceeesding to next file");
               this.deletedFileRetry = 0;
             } else {
               if (this.logger.isDebugEnabled())
                 this.logger.warn("Got exception while fetching file attribute for {" + newFile.getName() + "} the file could have been renamed, let the caller to retry again");
               return null;
             }
           }
           if (attr != null) {
             if (this.lastKey.equals(attr.fileKey().toString())) {
               if (attr.size() > position) {
                 if (this.logger.isDebugEnabled()) {
                   this.logger.debug("File {" + newFile.getName() + "} Key {" + this.lastKey + "} CurPos {" + position + "}:{" + (attr.size() - position) + "} more bytes to process");
                 }
                 this.positionToStart = position;
                 return newFile;
               }
               
 
 
 
 
 
 
               newFile = (File)this.fileList.poll();
 
             }
             else
             {
 
               if (this.logger.isDebugEnabled())
                 this.logger.debug("LastKey {" + this.lastKey + "} RefKey {" + attr.fileKey().toString() + "} Looks like the file is deleted or moved {" + file.getName() + "} key {" + this.lastKey + "}, let the caller to retry again");
               try {
                 Thread.sleep(10L);
               }
               catch (Exception sExp) {}
               return null;
             }
           } else {
             newFile = (File)this.fileList.poll();
           }
         }
       }
       
       if (newFile != null) {
         this.lastFileObj = newFile;
         this.lastKey = ((String)this.nameToKeyMap.get(newFile.getName()));
         if ((this.lastKey == null) && 
           (this.logger.isDebugEnabled())) {
           this.logger.warn("Null key found for {" + newFile.getName() + "}");
         }
         if (this.logger.isDebugEnabled())
           this.logger.debug("Giving {" + newFile.getName() + "} StartPos {" + this.positionToStart + "} for processing, key {" + this.lastKey + "} {" + this.fileList.size() + "} more files to process");
       } else {
         this.lastFileObj = null;
       }
     }
     return newFile;
   }
   
   public long getPosition() {
     return this.positionToStart;
   }
   
   public boolean position(String fileName)
   {
     synchronized (this.fileList) { File fileObj;
       while ((fileObj = (File)this.fileList.peek()) != null) {
         if ((fileObj != null) && (fileObj.getName().equals(fileName)))
           return true;
         fileObj = (File)this.fileList.poll();
       }
     }
     this.logger.warn("Couldn't find {" + fileName + "} for positioning");
     return false;
   }
   
 
   protected void postProcess(File file) {}
   
 
   protected void preProcess(File file) {}
   
   protected int fileListSize()
   {
     return 1;
   }
 }

