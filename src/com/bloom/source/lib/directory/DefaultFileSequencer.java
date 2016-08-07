 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.utils.DefaultFileComparator;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;
 
 
 
 
 public class DefaultFileSequencer
   extends FileSequencer
 {
   Logger logger = Logger.getLogger(DefaultFileSequencer.class);
   
   PriorityBlockingQueue<Object> fileList;
   File lastFile;
   long recoverPosition;
   Map<String, File> nameToObjMap;
   String lastAddedFile = "";
   
   public DefaultFileSequencer(Map<String, Object> map) throws IOException, AdapterException {
     super(map);
     this.fileList = new PriorityBlockingQueue(10, new DefaultFileComparator());
     this.nameToObjMap = new TreeMap();
   }
   
   protected int fileListSize() {
     return this.fileList.size();
   }
   
   public boolean isEmpty() {
     if (this.lastFile != null) {
       if (!this.lastFile.exists()) {
         return this.fileList.size() == 0;
       }
       return false;
     }
     return this.fileList.size() == 0;
   }
   
   public void onFileModify(File file) {
     if (!this.lastAddedFile.equals(file.getName())) {
       if (this.logger.isDebugEnabled())
         this.logger.debug("Got file modification event for {" + file.getName() + "}");
       this.lastAddedFile = file.getName();
       this.fileList.add(file);
     }
   }
   
   public void onFileDelete(File file) {
     File tmpFile = (File)this.nameToObjMap.get(file.getName());
     if (tmpFile != null) {
       this.fileList.remove(tmpFile);
       this.nameToObjMap.remove(file.getName());
     }
   }
   
   public void onFileCreate(File file) {
     if ((!this.lastAddedFile.equals(file.getName())) && (this.nameToObjMap.get(file.getName()) == null)) {
       this.nameToObjMap.put(file.getName(), file);
       this.fileList.add(file);
       this.lastAddedFile = file.getName();
     }
   }
   
   public boolean position(String fileName) {
     if (this.nameToObjMap.get(fileName) != null) {
       File file;
       while (((file = (File)this.fileList.peek()) != null) && (this.fileList.size() > 0)) {
         if (file != null) {
           String fName = file.getName();
           this.nameToObjMap.remove(fName);
           if (fName.equals(fileName))
             return true;
         }
         this.fileList.poll();
       }
     }
     return false;
   }
   
 
 
 
 
   public File getNextFile(File file, long position)
   {
     File nextFile = (File)this.fileList.poll();
     if (nextFile != null) {
       this.nameToObjMap.remove(nextFile.getName());
     }
     this.lastFile = nextFile;
     return nextFile;
   }
   
   public void onFileDelete(String fileKey, String fileName)
   {
     this.logger.error("onFileDelete() with FileKey based implementation is not required here");
   }
 }

