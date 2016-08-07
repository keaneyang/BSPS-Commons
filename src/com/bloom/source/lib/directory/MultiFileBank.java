 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.StandardWatchEventKinds;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;
 
 public class MultiFileBank
   extends FileBank
 {
   public MultiFileBank(Property prop)
     throws AdapterException
   {
     super(prop);
   }
   
   public void updateInstance(Property prop) throws AdapterException {
     if (getSequencer(prop.wildcard) == null) {
       FileSequencer sequencer = createSequencer(prop);
       this.sequencerMap.put(prop.wildcard, sequencer);
       instanceCnt.incrementAndGet();
     }
   }
   
   private void updateObserver(FileBank.Subject subject) {
     setChanged();
     notifyObservers(subject);
   }
   
   protected FileSequencer getSequencer(File file) {
     String pGroup = FileBank.extractProcessGroup(this.prop.groupPattern, file);
     FileSequencer sequencer = getSequencer(pGroup);
     if ((sequencer == null) && 
       (logger.isDebugEnabled())) {
       logger.debug("Wildcard {" + pGroup + "} has no assiciated sequencer");
     }
     return sequencer;
   }
   
   protected FileSequencer getSequencer(String wildcard) {
     FileSequencer sequencer = (FileSequencer)this.sequencerMap.get(wildcard);
     if (sequencer == null)
       logger.warn("No sequencer found for {" + wildcard + "}");
     return sequencer;
   }
   
   public boolean isEOFReached(String wildcard, InputStream in, boolean fileChanged) throws IOException {
     FileSequencer sequencer = getSequencer(wildcard);
     if (sequencer != null)
       return sequencer.isEOFReached(in, fileChanged);
     return true;
   }
   
   public boolean isEmpty(String wildcard) {
     FileSequencer sequencer = getSequencer(wildcard);
     if (sequencer != null)
       return sequencer.isEmpty();
     return true;
   }
   
   public FileBank.FileDetails getNextFile(String wildcard, File file, long position) {
     FileSequencer sequencer = getSequencer(wildcard);
     if (sequencer != null) {
       File fileObj = sequencer.getNextFile(file, position);
       if (fileObj != null) {
         FileBank.FileDetails fd = new FileBank.FileDetails();
         fd.file = fileObj;
         fd.startPosition = sequencer.getPosition();
         if (logger.isDebugEnabled())
           logger.debug("File process {" + fd.file.getName() + "} start position {" + fd.startPosition + "}");
         return fd;
       }
     }
     return null;
   }
   
   public long getPosition(String wildcard) {
     FileSequencer sequencer = getSequencer(wildcard);
     if (sequencer != null)
       return sequencer.getPosition();
     return 0L;
   }
   
   public boolean position(String wildcard, String fileToRecover) {
     FileSequencer sequencer = getSequencer(wildcard);
     if (sequencer != null)
       return sequencer.position(fileToRecover);
     return false;
   }
   
 
 
   public void onFileCreate(File file)
   {
     if (logger.isDebugEnabled())
       logger.debug("New file created {" + file.getName() + "}");
     this.subject.file = file;
     this.subject.event = StandardWatchEventKinds.ENTRY_CREATE;
     updateObserver(this.subject);
     FileSequencer sequencer = getSequencer(file);
     if (sequencer != null) {
       sequencer.onFileCreate(file);
     }
   }
   
   public void onFileDelete(File file) {
     if (logger.isDebugEnabled())
       logger.debug("File {" + file.getName() + "} is deleted");
     this.subject.file = file;
     this.subject.event = StandardWatchEventKinds.ENTRY_DELETE;
     updateObserver(this.subject);
     FileSequencer sequencer = getSequencer(file);
     if (sequencer != null) {
       sequencer.onFileDelete(file);
     }
   }
   
   public void onFileDelete(String fileKey, String fileName) {
     if (logger.isDebugEnabled())
       logger.debug("File {" + fileKey + "} {" + fileName + "} is deleted");
     this.subject.file = new File(fileName);
     this.subject.event = StandardWatchEventKinds.ENTRY_DELETE;
     updateObserver(this.subject);
     FileSequencer sequencer = getSequencer(fileName);
     if (sequencer != null) {
       sequencer.onFileDelete(fileKey, fileName);
     }
   }
   
   public void onFileModify(File file) {
     if (logger.isDebugEnabled())
       logger.debug("File {" + file.getName() + "} is modified");
     this.subject.file = file;
     this.subject.event = StandardWatchEventKinds.ENTRY_MODIFY;
     setChanged();
     updateObserver(this.subject);
     FileSequencer sequencer = getSequencer(file);
     if (sequencer != null) {
       sequencer.onFileModify(file);
     }
   }
 }

