#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

include Java
java_import org.apache.hadoop.hbase.util.Pair

# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class GroupAdmin
    include HBaseConstants

    def initialize(configuration, formatter)
      @admin = org.apache.hadoop.hbase.group.GroupAdminClient.new(configuration)
      @conf = configuration
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    # Returns a list of groups in hbase
    def listGroups
      @admin.listGroups.map { |g| g.getName }
    end
    #----------------------------------------------------------------------------------------------
    # get a group's information
    def getGroup(group_name)
      group = @admin.getGroupInfo(group_name)
      res = {}
      if block_given?
        yield("Servers:")
      else
        res += v
      end
      group.getServers.each do |v|
        if block_given?
          yield(v)
        else
          res += v
        end
      end
      if block_given?
        yield("Tables:")
      else
        res += v
      end
      group.getTables.each do |v|
        if block_given?
          yield(v)
        else
          res += v
        end
      end
    end
    #----------------------------------------------------------------------------------------------
    # add a group
    def addGroup(group_name)
      @admin.addGroup(group_name)
    end
    #----------------------------------------------------------------------------------------------
    # remove a group
    def removeGroup(group_name)
      @admin.removeGroup(group_name)
    end
    #----------------------------------------------------------------------------------------------
    # move server to a group
    def moveServers(dest, *args)
      servers = java.util.HashSet.new()
      args[0].each do |s|
        servers.add(s)
      end
      @admin.moveServers(servers, dest)
    end
    #----------------------------------------------------------------------------------------------
    # move server to a group
    def moveTables(dest, *args)
      tables = java.util.HashSet.new();
      args[0].each do |s|
        tables.add(s)
      end
      @admin.moveTables(tables,dest)
    end
    #----------------------------------------------------------------------------------------------
    # get group of server
    def getGroupOfServer(server)
      @admin.getGroupOfServer(server)
    end
    #----------------------------------------------------------------------------------------------
    # get list tables of groups
    def listTablesOfGroup(group_name)
      @admin.listTablesOfGroup(group_name).map { |g| g }
    end
    #----------------------------------------------------------------------------------------------
    # list servers in transition
    def listServersInTransition()
      iter = @admin.listServersInTransition.entrySet.iterator
      while iter.hasNext
        entry = iter.next
        if block_given?
          yield(entry.getKey, entry.getValue)
        else
          res[entry.getKey] = entry.getValue
        end
      end
    end
  end
end
