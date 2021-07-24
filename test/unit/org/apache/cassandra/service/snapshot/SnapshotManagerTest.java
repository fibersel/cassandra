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

package org.apache.cassandra.service.snapshot;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.function.Predicate;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(OrderedJUnit4ClassRunner.class)
public class SnapshotManagerTest extends CQLTester
{
    private static NodeProbe probe;

    private static void SSstart() throws Exception {
        StorageService.instance.initServer();
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @BeforeClass
    public static void setup() throws Exception
    {
        SSstart();
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    @Test
    public void testSnapshotIndexingAndRemoval() throws InterruptedException
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot", "--ttl", "1m", "-t", "some-name");

        PriorityQueue<TableSnapshotDetails> snapshots = StorageService.instance.snapshotManager.getExpiringSnapshots();
        Predicate<TableSnapshotDetails> tagPredicate = (details) -> details.getTag().equals("some-name");
        assertThat(snapshots.parallelStream().filter(tagPredicate).count()).isNotEqualTo(0L);

        Thread.sleep(70000);

        assertThat(snapshots.parallelStream().filter(tagPredicate).count()).isEqualTo(0L);
    }

    @Test
    public void testSnapshotsLoadOnStartup() throws Exception {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("snapshot", "--ttl", "10m", "-t", "some-name");

        PriorityQueue<TableSnapshotDetails> snapshots = StorageService.instance.snapshotManager.getExpiringSnapshots();
        Predicate<TableSnapshotDetails> tagPredicate = (details) -> details.getTag().equals("some-name");
        assertThat(snapshots.parallelStream().filter(tagPredicate).count()).isNotEqualTo(0L);

        StorageService.instance.snapshotManager.shutdown();
        StorageService.instance.snapshotManager.start();

        assertThat(snapshots.parallelStream().filter(tagPredicate).count()).isNotEqualTo(0L);
    }
}
