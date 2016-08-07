 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.io.common.HDFSCommon;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
 
 
 
 public class HDFSWatcher
   extends Watcher
 {
   protected FileSystem fsToBeWatched;
   protected String hadoopUri;
   protected long defaultBlockSize;
   protected HDFSCommon hdfsCommon;
   protected FileSystem hadoopFileSystem;
   protected Map<String, Object> fileMap;
   protected int pollInterval = 500;
   
   public HDFSWatcher(Property prop, WatcherCallback callback) throws IOException
   {
     super(prop, callback);
   }
   
   protected void init() throws AdapterException {
     this.hadoopUri = (this.prop.hadoopUrl + this.prop.wildcard);
     this.hdfsCommon = new HDFSCommon(this.prop);
     this.hadoopFileSystem = this.hdfsCommon.getHDFSInstance();
     this.defaultBlockSize = this.hadoopFileSystem.getConf().getLong("dfs.blocksize", 0L);
     this.fileMap = new HashMap();
   }
   
 
   public void run()
   {
     Path fileNameToBePolled = new Path(this.hadoopUri);
     
 
 
 
 
 
 
 
     while (!this.stopCalled) {
       try
       {
         FileStatus[] files = this.fsToBeWatched.globStatus(fileNameToBePolled);
         
         if (files != null) {
           for (FileStatus file : files) {
             if (file.isFile())
             {
 
 
 
 
               if ((file.getLen() == 0L) || (file.getLen() % this.defaultBlockSize == 0L)) {
                 Thread.sleep(this.prop.eofdelay);
               }
               else
               {
                 String fileName = file.getPath().getName();
                 if (!this.fileMap.containsKey(fileName)) {
                   this.fileMap.put(fileName, file.getPath());
                   Path path = file.getPath();
                   
                   if (this.logger.isDebugEnabled())
                     this.logger.debug("New file is added in the HDFS " + fileName);
                 }
               } }
           }
         }
       } catch (IllegalArgumentException|IOException|InterruptedException e) {
         AdapterException exp = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
         this.logger.error(exp.getErrorMessage());
       }
     }
     
 
 
 
     try
     {
       Thread.sleep(this.pollInterval);
     } catch (InterruptedException e) {
       AdapterException exp = new AdapterException(Error.GENERIC_INTERRUPT_EXCEPTION, e);
       this.logger.warn(exp.getErrorMessage());
     }
   }
 }

