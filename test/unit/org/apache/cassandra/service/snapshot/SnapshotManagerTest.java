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

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.collect.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.DefaultFSErrorHandler;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotManagerTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        FileUtils.setFSErrorHandler(new DefaultFSErrorHandler());
    }

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    public Set<File> createFolders() throws IOException {
        File folder = temporaryFolder.newFolder();
        Set<File> folders = new HashSet<>();
        for (String folderName : Arrays.asList("foo", "bar", "buzz")) {
            File subfolder = new File(folder, folderName);
            subfolder.mkdir();
            assertThat(subfolder).exists();
            folders.add(subfolder);
        };

        return folders;
    }


    private TableSnapshotDetails generateSnapshotDetails(String tag, Instant expiration) throws Exception {
        return new TableSnapshotDetails(
            "ks",
            "tbl",
            tag,
            new SnapshotManifest(
                Arrays.asList("db1", "db2", "db3"),
                Instant.EPOCH,
                expiration
            ),
            createFolders(),
            (file) -> 0L
        );
    }

    @Test
    public void testOnlyExpiringSnapshotsIndexing() throws Exception {
        ArrayList<TableSnapshotDetails> details = new ArrayList<>(Arrays.asList(
            generateSnapshotDetails("expired", Instant.EPOCH),
            generateSnapshotDetails("non-expired", Instant.now().plusMillis(5000)),
            generateSnapshotDetails("non-expiring", null)
        ));

        Function<String, Predicate<TableSnapshotDetails>> tagPredicate = (tagname) -> (dt) -> dt.getTag().equals(tagname);
        SnapshotManager manager = new SnapshotManager(3, 3, details::stream);
        manager.loadSnapshots();

        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("expired"))).hasSize(1);
        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("non-expired"))).hasSize(1);
        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("non-expiring"))).hasSize(0);
    }

    @Test
    public void testClearingOfExpiriedSnapshots() throws Exception {
        ArrayList<TableSnapshotDetails> details = new ArrayList<>(Arrays.asList(
            generateSnapshotDetails("expired", Instant.EPOCH),
            generateSnapshotDetails("non-expired", Instant.now().plusMillis(50000)),
            generateSnapshotDetails("non-expiring", null)
        ));

        Function<String, Predicate<TableSnapshotDetails>> tagPredicate = (tagname) -> (dt) -> dt.getTag().equals(tagname);
        SnapshotManager manager = new SnapshotManager(3, 3, details::stream);
        manager.loadSnapshots();

        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("expired"))).hasSize(1);
        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("non-expired"))).hasSize(1);
        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("non-expiring"))).hasSize(0);

        manager.clearExpiredSnapshots();

        assertThat(details.get(0).isDeleted()).isEqualTo(true);
        assertThat(details.get(1).isDeleted()).isEqualTo(false);
        assertThat(details.get(2).isDeleted()).isEqualTo(false);
    }

    @Test
    public void testSnapshotManagerScheduler() throws Exception {
        ArrayList<TableSnapshotDetails> details = new ArrayList<>(Arrays.asList(
            generateSnapshotDetails("expired", Instant.now().plusMillis(1000)),
            generateSnapshotDetails("non-expiring", null)
        ));

        Function<String, Predicate<TableSnapshotDetails>> tagPredicate = (tagname) -> (dt) -> dt.getTag().equals(tagname);
        SnapshotManager manager = new SnapshotManager(1, 3, details::stream);
        manager.start();

        Thread.sleep(4000);

        assertThat(details.get(0).isDeleted()).isEqualTo(true);
        assertThat(details.get(1).isDeleted()).isEqualTo(false);
        assertThat(manager.getExpiringSnapshots()).isEmpty();
    }
}
