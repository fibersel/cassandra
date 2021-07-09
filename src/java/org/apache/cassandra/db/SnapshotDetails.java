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
package org.apache.cassandra.db;

import java.time.Instant;
import java.io.File;
import java.io.IOException;
import org.apache.cassandra.io.util.FileUtils;
import java.util.Map;
import org.apache.cassandra.config.Duration;



public class SnapshotDetails {
    public String tag;
    public String keyspace;
    public Instant createdAt;
    public Instant expiresAt;

    public SnapshotDetails(String tag, String keyspace, File manifestFile) {
        this.tag = tag;
        this.keyspace = keyspace;
        try
        {
            Map<String, Object> manifest = FileUtils.readFileToJson(manifestFile);
            if (manifest.containsKey("created_at")) {
                this.createdAt = Instant.parse((String)manifest.get("created_at"));
            }
            if (manifest.containsKey("expires_at")) {
                this.expiresAt = Instant.parse((String)manifest.get("expires_at"));
            }
        } catch (IOException e) {
            //
        }
    }


    public SnapshotDetails(String tag, String keyspace, Duration ttl) {
        this.tag = tag;
        this.keyspace = keyspace;
        this.createdAt = Instant.now();
        this.expiresAt = createdAt.plusMillis(ttl.toMilliseconds());
    }

    public boolean isExpired() {
        if (createdAt == null || expiresAt == null) {
            return false;
        }
        Instant now = Instant.now();

        return expiresAt.compareTo(now) < 0;
    }
}