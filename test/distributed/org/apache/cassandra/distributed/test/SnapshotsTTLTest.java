/*
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

package org.apache.cassandra.distributed.test;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

public class SnapshotsTTLTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void before() throws IOException
    {
        cluster = init(Cluster.build()
                              .withNodes(2)
                              .withDCs(2)
                              .start());
    }

    @AfterClass
    public static void after()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testSnapshotsCleanupByTTL() throws Exception {
        cluster.get(1).nodetoolResult("snapshot", "--ttl", "1m", "-t", "basic").asserts().success();
        cluster.get(1).nodetoolResult("listsnapshots").asserts().success().stdoutContains("basic");

        Thread.sleep(80000);
        cluster.get(1).nodetoolResult("listsnapshots").asserts().success().stdoutNotContains("basic");
    }
}
