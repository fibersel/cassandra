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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Duration;
import org.apache.cassandra.io.util.FileUtils;

public class TableSnapshotTest
{
    @Before
    public void setup() {
        DatabaseDescriptor.daemonInitialization();
    }

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    public Set<File> createFolders() throws IOException {
        File folder = tempFolder.newFolder();
        Set<File> folders = new HashSet<>();
        for (String folderName : Arrays.asList("foo", "bar", "buzz")) {
            File subfolder = new File(folder, folderName);
            subfolder.mkdir();
            assertThat(subfolder).exists();
            folders.add(subfolder);
        };

        return folders;
    }

    @Test
    public void testSnapshotDirsRemoval() throws IOException
    {
        Set<File> folders = createFolders();

        TableSnapshot tableDetails = new TableSnapshot(
        "ks",
        "tbl",
        "some",
        new SnapshotManifest(Arrays.asList("db1", "db2", "db3"), new Duration("3m")),
        folders,
        (File file) -> { return 0L; }
        );

        tableDetails.deleteSnapshot();

        for (File file : folders) {
            assertThat(file).doesNotExist();
        }
    }

    private Long writeBatchToFile(File file) throws IOException {
        FileOutputStream out = new FileOutputStream(file);
        out.write(1);
        out.write(2);
        out.write(3);
        out.close();
        return 3L;
    }

    @Test
    public void testComputeSizeOnDisk() throws IOException
    {
        Set<File> folders = createFolders();

        TableSnapshot tableDetails = new TableSnapshot(
        "ks",
        "tbl",
        "some",
        new SnapshotManifest(Arrays.asList("db1", "db2", "db3"), new Duration("3m")),
        folders,
        (File file) -> { return 0L; }
        );

        Long res = 0L;

        for (File dir : folders) {
            writeBatchToFile(new File(dir, "tmp"));
            res += FileUtils.folderSize(dir);
        }

        assertThat(tableDetails.computeSizeOnDiskBytes()).isEqualTo(res);
    }

    @Test
    public void testComputeTrueSize() throws IOException
    {
        Set<File> folders = createFolders();

        TableSnapshot tableDetails = new TableSnapshot(
        "ks",
        "tbl",
        "some",
        new SnapshotManifest(Arrays.asList("db1", "db2", "db3"), new Duration("3m")),
        folders,
        File::length
        );

        Long res = 0L;

        for (File dir : folders) {
            res += dir.length();
        }

        assertThat(tableDetails.computeTrueSizeBytes()).isEqualTo(res);
    }
}
