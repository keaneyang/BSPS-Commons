package com.bloom.source.lib.io.common;

import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public class HDFSCommon
{
  private Property property;
  private String hadoopUrl;
  private Logger logger = Logger.getLogger(HDFSCommon.class);
  private boolean kerberosEnabled;
  private String authenticationName;
  private String principal;
  private String keytabPath;
  
  public HDFSCommon(Property property)
  {
    this.property = property;
  }
  
  public org.apache.hadoop.fs.FileSystem getHDFSInstance()
    throws AdapterException
  {
    this.hadoopUrl = this.property.hadoopUrl;
    try
    {
      URI uri = new URI(this.hadoopUrl);
      String scheme = uri.getScheme();
      if (scheme != null)
      {
        if ((!scheme.equalsIgnoreCase("hdfs")) && (!scheme.equalsIgnoreCase("maprfs"))) {
          throw new AdapterException("Specified HDFS URL " + this.hadoopUrl + " with scheme " + scheme + " is not supported");
        }
      }
      else {
        throw new AdapterException("Invalid HDFS URL " + this.hadoopUrl + " is specified");
      }
    }
    catch (URISyntaxException e1)
    {
      throw new AdapterException("Failure in constructing URL", e1);
    }
    Configuration hdfsConfiguration = new Configuration();
    
    String hadoopConfigurationPath = this.property.hadoopConfigurationPath;
    if (hadoopConfigurationPath != null)
    {
      String separator = FileSystems.getDefault().getSeparator();
      if (!hadoopConfigurationPath.endsWith(separator)) {
        hadoopConfigurationPath = hadoopConfigurationPath + separator;
      }
      try
      {
        org.apache.hadoop.fs.FileSystem localFileSystem = org.apache.hadoop.fs.FileSystem.getLocal(hdfsConfiguration);
        Path coreSitePath = new Path(hadoopConfigurationPath + "core-site.xml");
        if (!localFileSystem.exists(coreSitePath)) {
          this.logger.warn("Specified hadoop configuration path " + hadoopConfigurationPath + " doesn't have core-site.xml file. It is recommended" + " to have this configuration as part of the client. Please make sure correct hadoop configuration path is specified");
        } else {
          hdfsConfiguration.addResource(coreSitePath);
        }
        Path hdfsSitePath = new Path(hadoopConfigurationPath + "hdfs-site.xml");
        if (!localFileSystem.exists(hdfsSitePath)) {
          this.logger.warn("Specified hadoop configuration path " + hadoopConfigurationPath + " doesn't have hdfs-site.xml file. It is recommended" + " to have this configuration as part of the client. Please make sure correct hadoop configuration path is specified");
        } else {
          hdfsConfiguration.addResource(hdfsSitePath);
        }
      }
      catch (IOException e)
      {
        throw new AdapterException("Failure in retreiving LocalFileSystem to load resources from the specified configuration path " + hadoopConfigurationPath, e);
      }
    }
    hdfsConfiguration.set("fs.defaultFS", this.hadoopUrl);
    hdfsConfiguration.setBoolean("dfs.support.append", true);
    hdfsConfiguration.setBoolean("fs.hdfs.impl.disable.cache", true);
    String authenticationPolicy = this.property.authenticationPolicy;
    if (authenticationPolicy != null) {
      validateAuthenticationPolicy(authenticationPolicy);
    }
    if (this.kerberosEnabled)
    {
      UserGroupInformation.setConfiguration(hdfsConfiguration);
      try
      {
        UserGroupInformation.loginUserFromKeytab(this.principal, this.keytabPath);
      }
      catch (IOException e)
      {
        AdapterException se = new AdapterException("Problem authenticating kerberos", e);
        throw se;
      }
    }
    org.apache.hadoop.fs.FileSystem hadoopFileSystem;
    try
    {
      hadoopFileSystem = org.apache.hadoop.fs.FileSystem.get(hdfsConfiguration);
      if (this.logger.isInfoEnabled()) {
        this.logger.info("HDFS is successfully initialized for " + this.hadoopUrl);
      }
    }
    catch (IOException e)
    {
      AdapterException exp = new AdapterException(Error.INVALID_HDFS_CONFIGURATION, e);
      throw exp;
    }
    return hadoopFileSystem;
  }
  
  public String getHadoopUrl()
  {
    return this.hadoopUrl;
  }
  
  private void validateAuthenticationPolicy(String authenticationPolicy)
    throws AdapterException
  {
    Map<String, Object> authenticationPropertiesMap = extractAuthenticationProperties(authenticationPolicy);
    if (this.authenticationName.equalsIgnoreCase("kerberos"))
    {
      this.kerberosEnabled = true;
      this.principal = ((String)authenticationPropertiesMap.get("principal"));
      this.keytabPath = ((String)authenticationPropertiesMap.get("keytabpath"));
      if ((this.principal == null) && (this.principal.trim().isEmpty()) && (this.keytabPath == null) && (this.keytabPath.trim().isEmpty())) {
        throw new AdapterException("Principal or Keytab path required for kerberos authentication cannot be empty or null");
      }
    }
    else
    {
      throw new AdapterException("Specified authentication " + this.authenticationName + " is not supported");
    }
  }
  
  private Map<String, Object> extractAuthenticationProperties(String authenticationPolicyName)
  {
    Map<String, Object> authenticationPropertiesMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    String[] extractedValues = authenticationPolicyName.split(",");
    boolean isAuthenticationPolicyNameExtracted = false;
    for (String value : extractedValues)
    {
      String[] properties = value.split(":");
      if (properties.length > 1)
      {
        authenticationPropertiesMap.put(properties[0], properties[1]);
      }
      else if (!isAuthenticationPolicyNameExtracted)
      {
        this.authenticationName = properties[0];
        isAuthenticationPolicyNameExtracted = true;
      }
    }
    return authenticationPropertiesMap;
  }
}
