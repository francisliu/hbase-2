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

module Shell
  module Commands
    class ShutdownCluster < Command
      def help
        return <<-EOF
Shutdown the hbase cluster.
Syntax : shutdown_cluster 'yes'
EOF
      end

      def command(verify)
        if verify.eql?("yes") then
          admin.shutdownCluster
          formatter.row(['Shutdown sent.'])
        else
          formatter.row(['Argument is not \'yes\', ignoring request.'])
        end
      end
    end
  end
end
