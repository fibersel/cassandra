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
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.DefaultFSErrorHandler;

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


    private TableSnapshot generateSnapshotDetails(String tag, Instant expiration) throws Exception {
        return new TableSnapshot(
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
        ArrayList<TableSnapshot> details = new ArrayList<>(Arrays.asList(
            generateSnapshotDetails("expired", Instant.EPOCH),
            generateSnapshotDetails("non-expired", Instant.now().plusMillis(5000)),
            generateSnapshotDetails("non-expiring", null)
        ));

        Function<String, Predicate<TableSnapshot>> tagPredicate = (tagname) -> (dt) -> dt.getTag().equals(tagname);
        SnapshotManager manager = new SnapshotManager(3, 3, details::stream);
        manager.loadSnapshots();

        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("expired"))).hasSize(1);
        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("non-expired"))).hasSize(1);
        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("non-expiring"))).hasSize(0);
    }

    @Test
    public void testClearingOfExpiredSnapshots() throws Exception {
        ArrayList<TableSnapshot> details = new ArrayList<>(Arrays.asList(
            generateSnapshotDetails("expired", Instant.EPOCH),
            generateSnapshotDetails("non-expired", Instant.now().plusMillis(50000)),
            generateSnapshotDetails("non-expiring", null)
        ));

        Function<String, Predicate<TableSnapshot>> tagPredicate = (tagname) -> (dt) -> dt.getTag().equals(tagname);
        SnapshotManager manager = new SnapshotManager(3, 3, details::stream);
        manager.loadSnapshots();

        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("expired"))).hasSize(1);
        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("non-expired"))).hasSize(1);
        assertThat(manager.getExpiringSnapshots().stream().filter(tagPredicate.apply("non-expiring"))).hasSize(0);

        manager.clearExpiredSnapshots();

        assertThat(details.get(0).exists()).isFalse();
        assertThat(details.get(1).exists()).isTrue();
        assertThat(details.get(2).exists()).isTrue();
    }

    @Test
    public void testSnapshotManagerScheduler() throws Exception {
        ArrayList<TableSnapshot> details = new ArrayList<>(Arrays.asList(
            generateSnapshotDetails("expired", Instant.now().plusMillis(1000)),
            generateSnapshotDetails("non-expiring", null)
        ));

        Function<String, Predicate<TableSnapshot>> tagPredicate = (tagname) -> (dt) -> dt.getTag().equals(tagname);
        SnapshotManager manager = new SnapshotManager(1, 3, details::stream);
        manager.start();

        Thread.sleep(4000);

        assertThat(details.get(0).exists()).isFalse();
        assertThat(details.get(1).exists()).isTrue();
        assertThat(manager.getExpiringSnapshots()).isEmpty();
    }

    @Test
    public void testDeleteSnapshot() throws Exception
    {
        // Given
        SnapshotManager manager = new SnapshotManager(1, 3, Stream::empty);
        TableSnapshot expiringSnapshot = generateSnapshotDetails("snapshot", Instant.now().plusMillis(50000));
        manager.addSnapshot(expiringSnapshot);
        assertThat(manager.getExpiringSnapshots()).contains(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isTrue();

        // When
        manager.clearSnapshot(expiringSnapshot);

        // Then
        assertThat(manager.getExpiringSnapshots()).doesNotContain(expiringSnapshot);
        assertThat(expiringSnapshot.exists()).isFalse();
    }
}
