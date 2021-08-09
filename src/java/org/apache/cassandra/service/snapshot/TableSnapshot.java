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
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.FileUtils;

public class TableSnapshot
{
    private final String keyspace;
    private final String table;
    private final String tag;

    private final Instant createdAt;
    private final Instant expiresAt;

    private final Set<File> snapshotDirs;
    private final Function<File, Long> trueDiskSizeComputer;

    public TableSnapshot(String keyspace, String table, String tag, SnapshotManifest manifest,
                         Set<File> snapshotDirs, Function<File, Long> trueDiskSizeComputer)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tag = tag;
        this.createdAt = manifest == null ? null : manifest.createdAt;
        this.expiresAt = manifest == null ? null : manifest.expiresAt;
        this.snapshotDirs = snapshotDirs;
        this.trueDiskSizeComputer = trueDiskSizeComputer;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getTable()
    {
        return table;
    }

    public String getTag()
    {
        return tag;
    }

    public Instant getCreatedAt()
    {
        return createdAt;
    }

    public Instant getExpiresAt()
    {
        return expiresAt;
    }

    public boolean isExpired(Instant now) {
        if (createdAt == null || expiresAt == null) {
            return false;
        }

        return expiresAt.compareTo(now) < 0;
    }

    public boolean exists() {
        return snapshotDirs.stream().anyMatch(s -> s.exists());
    }

    public boolean isExpiring() {
        return expiresAt != null;
    }

    public long computeSizeOnDiskBytes()
    {
        return snapshotDirs.stream().mapToLong(FileUtils::folderSize).sum();
    }

    public long computeTrueSizeBytes()
    {
        return snapshotDirs.stream().mapToLong(trueDiskSizeComputer::apply).sum();
    }

    public Collection<File> getDirectories()
    {
        return snapshotDirs;
    }

    @Override
    public String toString()
    {
        return "TableSnapshot{" +
               "keyspace='" + keyspace + '\'' +
               ", table='" + table + '\'' +
               ", tag='" + tag + '\'' +
               ", createdAt=" + createdAt +
               ", expiresAt=" + expiresAt +
               ", snapshotDirs=" + snapshotDirs +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableSnapshot that = (TableSnapshot) o;
        return Objects.equals(keyspace, that.keyspace) && Objects.equals(table, that.table) && Objects.equals(tag, that.tag) && Objects.equals(createdAt, that.createdAt) && Objects.equals(expiresAt, that.expiresAt) && Objects.equals(snapshotDirs, that.snapshotDirs);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, table, tag, createdAt, expiresAt, snapshotDirs);
    }
}
