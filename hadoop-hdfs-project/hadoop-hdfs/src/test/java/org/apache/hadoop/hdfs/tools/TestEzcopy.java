/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.tools;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEzcopy {
    private Configuration conf = null;
    private MiniDFSCluster cluster = null;
    private DistributedFileSystem srcDFS = null;
    private DistributedFileSystem dstDFS = null;

    private long BLOCKSIZE = 1024;
    private short REPLICATION = 1;

    private Path file = new Path("/foo/test");
    private Path dst = new Path("/bar/test");

    @Before
    public void setup() throws IOException {
        try {
            conf = new HdfsConfiguration();
            conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);

            cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();

            cluster.waitActive();

            srcDFS = cluster.getFileSystem(0);
            dstDFS = cluster.getFileSystem(0);

            DFSTestUtil.createFile(srcDFS, file, 2 * BLOCKSIZE, REPLICATION, 0L);

        } catch (Exception e) {
            cluster.shutdown();
            cluster = null;
        }
    }

    @After
    public void shutdown() throws Exception {
        if (cluster != null) {
            cluster.shutdown();
        }
    }

    @Test
    public void testezcopy() throws Exception {
        srcDFS.getFileChecksum(file);

        Ezcopy ez = new Ezcopy(conf, dstDFS);

        List<Ezcopy.ezcopyRequest> requests = new ArrayList<Ezcopy.ezcopyRequest>();


        requests.add(new Ezcopy.ezcopyRequest(file.toString(), dst.toString(), srcDFS, dstDFS));

        ez.copy(requests);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertTrue(dstDFS.isFile(dst));
        dstDFS.getFileChecksum(dst);
    }

}