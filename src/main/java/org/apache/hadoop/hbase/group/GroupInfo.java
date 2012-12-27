/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.group;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Sets;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Stores the group information of region server groups.
 */
public class GroupInfo implements Serializable {

	public static final String DEFAULT_GROUP = "default";
  public static final String TABLEDESC_PROP_GROUP = "group";
  public static final String OFFLINE_DEFAULT_GROUP = "_offline_default";
  public static final String TRANSITION_GROUP_PREFIX = "_transition_";

  private String name;
  private NavigableSet<String> servers;
  private NavigableSet<String> tables;

  public GroupInfo(String name) {
    this(name, Sets.<String>newTreeSet(), Sets.<String>newTreeSet());
	}

  //constructor for jackson
  @JsonCreator
  GroupInfo(@JsonProperty("name") String name,
            @JsonProperty("servers") NavigableSet<String> servers,
            @JsonProperty("tables") NavigableSet<String> tables) {
		this.name = name;
    this.servers = servers;
    this.tables = tables;
	}

  public GroupInfo(GroupInfo src) {
    name = src.getName();
    servers = Sets.newTreeSet(src.getServers());
    tables = Sets.newTreeSet(src.getTables());
  }

	/**
	 * Get group name.
	 *
	 * @return
	 */
	public String getName() {
		return name;
	}

	/**
	 * Adds the server to the group.
	 *
	 * @param hostPort the server
	 */
	public void addServer(String hostPort){
		this.servers.add(hostPort);
	}

	/**
	 * Adds a group of servers.
	 *
	 * @param hostPort the servers
	 */
	public void addAllServers(Collection<String> hostPort){
		this.servers.addAll(hostPort);
	}

	public boolean containsServer(String hostPort) {
    return servers.contains(hostPort);
	}

	/**
	 * Checks based of equivalence of host name and port.
	 *
	 * @param serverList The list to check for containment.
	 * @return true, if successful
	 */
	public boolean containsServer(Set<String> serverList) {
		if (serverList.size() == 0) {
			return false;
		} else {
			boolean contains = true;
			for (String hostPort : serverList) {
				contains = contains && this.getServers().contains(hostPort);
				if (!contains)
					return contains;
			}
			return contains;
		}
	}


	/**
	 * Get a copy of servers.
	 *
	 * @return
	 */
	public NavigableSet<String> getServers() {
		return servers;
	}

	/**
	 * Remove a server from this group.
	 *
	 * @param hostPort
	 */
	public boolean removeServer(String hostPort) {
    return this.servers.remove(hostPort);
	}

  /**
   * Set of tables that are members of this group
   * @return
   */
  public NavigableSet<String> getTables() {
    return tables;
  }

  public void addTable(String table) {
    tables.add(table);
  }

  public void addAllTables(Collection<String> arg) {
    tables.addAll(arg);
  }

  public boolean containsTable(String table) {
    return tables.contains(table);
  }

  public boolean removeTable(String table) {
    return tables.remove(table);
  }

  @Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{GroupName:");
		sb.append(this.name);
		sb.append("-");
		sb.append(" Severs:");
		sb.append(this.servers+ "}");
		return sb.toString();

	}

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GroupInfo groupInfo = (GroupInfo) o;

    if (!name.equals(groupInfo.name)) return false;
    if (!servers.equals(groupInfo.servers)) return false;
    if (!tables.equals(groupInfo.tables)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = servers.hashCode();
    result = 31 * result + tables.hashCode();
    result = 31 * result + name.hashCode();
    return result;
  }

}
