 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class HDFSDirectory
   extends Directory
 {
   int pollInterval;
   FileSystem hadoopFileSystem;
   String hadoopUri;
   Map<String, Object> fileMap;
   long defaultBlockSize;
   
   public HDFSDirectory(Property prop, FileSystem hadoopFileSystem, Comparator<Object> comparator)
     throws AdapterException, IOException, InterruptedException
   {
     super(comparator);
     this.fileMap = new HashMap();
     this.hadoopFileSystem = hadoopFileSystem;
     this.hadoopUri = (prop.hadoopUrl + prop.wildcard);
     initializeFileList();
     this.pollInterval = 500;
     this.defaultBlockSize = hadoopFileSystem.getConf().getLong("dfs.blocksize", 0L);
     
 
 
     this.WatcherThread = new Thread(new HDFSWatcher(hadoopFileSystem, prop));
     this.WatcherThread.setName("HDFSWatcherThread" + threadCount++);
     this.WatcherThread.start();
   }
   
 
 
 
 
 
   private void initializeFileList()
     throws AdapterException
   {
     try
     {
       FileStatus[] files = this.hadoopFileSystem.globStatus(new Path(this.hadoopUri));
       if (files != null) {
         for (FileStatus file : files) {
           if (file.isFile()) {
             String fileName = file.getPath().getName();
             this.fileMap.put(fileName, file.getPath());
             this.fileList.add(file.getPath());
           }
         }
       }
     }
     catch (IllegalArgumentException|IOException e)
     {
       AdapterException exp = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
       throw exp;
     }
   }
   
 
 
 
   private class HDFSWatcher
     implements Runnable
   {
     FileSystem fsToBeWatched;
     
 
 
     Property prop;
     
 
 
     public HDFSWatcher(FileSystem fsToBeWatched, Property prop)
     {
       this.fsToBeWatched = fsToBeWatched;
       this.prop = prop;
     }
     
 
     public void run()
     {
       Path fileNameToBePolled = new Path(HDFSDirectory.this.hadoopUri);
       
 
 
 
 
 
 
 
       while (!HDFSDirectory.this.finished)
       {
         try {
           FileStatus[] files = this.fsToBeWatched.globStatus(fileNameToBePolled);
           
           if (files != null) {
             for (FileStatus file : files) {
               if (file.isFile())
               {
 
 
 
 
                 if ((file.getLen() == 0L) || (file.getLen() % HDFSDirectory.this.defaultBlockSize == 0L)) {
                   Thread.sleep(this.prop.eofdelay);
                 }
                 else
                 {
                   String fileName = file.getPath().getName();
                   if (!HDFSDirectory.this.fileMap.containsKey(fileName)) {
                     HDFSDirectory.this.fileMap.put(fileName, file.getPath());
                     HDFSDirectory.this.fileList.add(file.getPath());
                     if (Directory.logger.isDebugEnabled())
                       Directory.logger.debug("New file is added in the HDFS " + fileName);
                   }
                 } }
             }
           }
         } catch (IllegalArgumentException|IOException|InterruptedException e) {
           Directory.logger.debug("Got exception while getting file list");
         }
         
 
         try
         {
           Thread.sleep(HDFSDirectory.this.pollInterval);
         } catch (InterruptedException e) {
           AdapterException exp = new AdapterException(Error.GENERIC_INTERRUPT_EXCEPTION, e);
           Directory.logger.warn(exp.getErrorMessage());
         }
       }
     }
   }
 }

