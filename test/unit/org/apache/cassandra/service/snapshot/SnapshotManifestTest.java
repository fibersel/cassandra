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
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.cassandra.config.Duration;
import org.codehaus.jackson.map.ObjectMapper;

public class SnapshotManifestTest
{
    @Test
    public void testNotExistingFileDeserialize() {
        assertThatIOException().isThrownBy(
            () -> {
                SnapshotManifest.deserializeFromJsonFile(new File("invalid"));
            });
    }

    @Test
    public void testDeserializeManifest() throws IOException
    {
        Map<String, Object> map = new HashMap<>();
        String createdAt = "2021-07-03T10:37:30Z";
        String expiresAt = "2021-08-03T10:37:30Z";
        map.put("created_at", createdAt);
        map.put("expires_at", expiresAt);
        map.put("files", Arrays.asList("db1", "db2", "db3"));

        ObjectMapper mapper = new ObjectMapper();
        File manifestFile = Paths.get("manifest.json").toFile();
        mapper.writeValue(manifestFile, map);
        SnapshotManifest manifest = SnapshotManifest.deserializeFromJsonFile(manifestFile);

        assertThat(manifest.getExpiresAt()).isEqualTo(Instant.parse(expiresAt));
        assertThat(manifest.getCreatedAt()).isEqualTo(Instant.parse(createdAt));
    }

    @Test
    public void testSerializeAndDeserialize() throws Exception {
        SnapshotManifest manifest = new SnapshotManifest(Arrays.asList("db1", "db2", "db3"), new Duration("2m"));
        File file = new File("manifest.json");
        manifest.serializeToJsonFile(file);
        manifest = SnapshotManifest.deserializeFromJsonFile(file);
        assert manifest.createdAt != null;
        assert manifest.expiresAt != null;
        assert manifest.files != null && manifest.files.size() == 3;
    }
}
