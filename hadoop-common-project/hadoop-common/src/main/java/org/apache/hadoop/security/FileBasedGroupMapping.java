package org.apache.hadoop.security;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.HostsFileReader;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is an implementation for GroupMappingServiceProvider based on configuration file.
 * The configuration file has multiple lines, each for one user, using the following format: 
 * <p>
 * user_name:group1,group2,group3:IP1,IP2,IP3
 * </p>
 * It uses the following order to fetch the user-group mapping:
 * <ol>
 * <li>Read allow slave nodes' ips from the host file and extra host file, and
 * put them in a set. The host file path is configured using "dfs.hosts" and extra
 * host file path using "hadoop.security.group.mapping.extrahosts".</li>
 * <li>Read the mentioned configuration file into memory to construct two maps, 
 * user->set(ip), and user->list(groups). The file path is configured using 
 * "hadoop.security.group.mapping.user2groupfile". </li>
 * <li>When trying to find the groups for a user, the client's ip is first checked. 
 * If its ip is inside the slave host set generated in step 1, jump to step 5.</li>
 * <li>If the client's ip is not contained in the allowed ip set generated in step 2,
 * stop it.</li>
 * <li>Return its groups from user->list(grops) mapping.</li>
 * <li>If refresh is needed, redo the first two steps. </li>
 * </ol>
 * Note the order of user-group refresh when trying to add or remove nodes in the cluster.
 * Remember to change the configuration hadoop.security.groups.cache.secs 
 */
public class FileBasedGroupMapping implements GroupMappingServiceProvider {
  
  private static final Log LOG = LogFactory.getLog(FileBasedGroupMapping.class);
  Configuration conf = new Configuration();
  Map<String, Set<String>> user2ip = new ConcurrentHashMap<String, Set<String>>();
  Map<String, List<String>> user2group = new ConcurrentHashMap<String, List<String>>();
  Set<String> innerNodes = new HashSet<String>();
  
  public FileBasedGroupMapping() {
    try {
      cacheGroupsRefresh();
    } catch (IOException e) {
      LOG.error("Error happens when loading user mapping file! ", e);
    }
  }

  @Override
  public List<String> getGroups(String user) throws AccessControlException {
    String clientIP = null;
    if(Server.getRemoteIp() != null){
      clientIP = Server.getRemoteIp().getHostAddress();
    }
    // No need to check returned value. If user not allowed, throws exception. 
    isUserLocationAllowed(user, clientIP);
    if(!user2group.containsKey(user))
      throw new AccessControlException("Permission denied. User " + user + " not found!");
    if(user2group.get(user).size() == 0)
      return new ArrayList<String>();
    return user2group.get(user);
  }
  
  /**
   * Check whether the user came from the allowed IP.  
   * @param user 
   * @param clientIP 
   * @return
   * @throws AccessControlException
   */
  public boolean isUserLocationAllowed(String user, String clientIP) throws AccessControlException {
    if(clientIP != null && !innerNodes.contains(clientIP)){
      if (!user2ip.containsKey(user))
        throw new AccessControlException("Permission denied. User " + user
                                          + " not found!"); 
      if (!user2ip.get(user).contains(clientIP))
        throw new AccessControlException("Permission denied. User " + user
                                          + " is not allowed from ip " + clientIP);
    }
    return true;
  }

  /**
   * Is the user name present in config file?
   * @param username
   * @return
   */
  public boolean isUserPresent(String username) {
    return user2group.containsKey(username);
  }
 
  @Override
  public void cacheGroupsRefresh() throws IOException {
    loadConfiguration();
  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {
    // I ignore this call
  }
  
  private void loadConfiguration() throws IOException{
    HostsFileReader hostsReader = new HostsFileReader(conf.get("dfs.hosts",""),
        conf.get("dfs.hosts.exclude",""));
    // these are hdfs slaves
    Set<String> innerNodesTemp = hostsReader.getHosts();
    hostsReader = new HostsFileReader(conf.get("mapred.hosts",""),
        conf.get("dfs.hosts.exclude",""));
    // these are mapred hosts to add
    innerNodesTemp.addAll(hostsReader.getHosts());
    hostsReader = new HostsFileReader(conf.get("hadoop.security.group.mapping.extrahosts",""),
        conf.get("dfs.hosts.exclude",""));
    // these are extra hosts to add, such as NN and JT
    innerNodesTemp.addAll(hostsReader.getHosts());
    ConcurrentHashMap<String, Set<String>> user2ipTemp = new ConcurrentHashMap<String, Set<String>>();
    ConcurrentHashMap<String, List<String>> user2groupTemp = new ConcurrentHashMap<String, List<String>>();
    readUserGroupMapping(conf.get("hadoop.security.group.mapping.user2groupfile",""), 
        user2ipTemp, 
        user2groupTemp);
    synchronized(this){
      user2ip = user2ipTemp;
      user2group = user2groupTemp;
      innerNodes = innerNodesTemp;
    }
    LOG.info("Now the user-group mapping is:");
    for(String user : user2ip.keySet())
      LOG.info("\tuser "+user+", IPs "+(user2ip.get(user)).toString());
    for(String user : user2group.keySet())
      LOG.info("\tuser "+user+", Groups "+(user2group.get(user)).toString());
    LOG.info("\tinnerNodes "+innerNodes);
  }
  
  private void readUserGroupMapping(String fileName, 
      Map<String, Set<String>> user2ip,  
      Map<String, List<String>> user2group) throws IOException{
    File file = new File(fileName);
    if (!file.exists()) {
      throw new IOException(fileName+" cannot be found! Please recheck your settings!");
    }
    FileInputStream fis = new FileInputStream(file);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(fis));
      String line;
      while ((line = reader.readLine()) != null) {
        // if this is comment line or empty line
        if(line.startsWith("#") || line.trim().isEmpty())
          continue;
        String[] segs = line.split(":");
        if (segs != null && segs.length == 3) {
          String user = segs[0];
          String groups[] = segs[1].split(",");
          for(int i = 0; i < groups.length; ++i)
            groups[i]=groups[i].trim();
          String ips[] = segs[2].split(",");
          for(int i = 0; i < ips.length; ++i)
            ips[i] = ips[i].trim();
          user2ip.put(user, new HashSet<String>(Arrays.asList(ips)));
          List<String> existGroup = user2group.get(user);
          if(existGroup == null) {
            existGroup = new ArrayList<String>();
          }
          existGroup.addAll(Arrays.asList(groups));
          user2group.put(user, existGroup);
        }
      }   
    } finally {
      if (reader != null) {
        reader.close();
      }
      fis.close();
    }  
  }

}

