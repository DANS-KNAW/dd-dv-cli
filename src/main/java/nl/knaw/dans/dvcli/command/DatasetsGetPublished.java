/*
 * Copyright (C) 2026 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.dvcli.command;

import nl.knaw.dans.lib.dataverse.DatabaseApi;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.time.OffsetDateTime;
import java.util.concurrent.Callable;

@Command(
    name = "datasets-get-published",
    description = "Get published datasets based on various filters",
    mixinStandardHelpOptions = true
)
public class DatasetsGetPublished extends AbstractDatabaseCmd implements Callable<Integer> {
    private final DatabaseApi dbApi;

    public static class DatasetVersionInfo {
        private final String pid;
        private final Long majorVersion;
        private final Long minorVersion;

        public DatasetVersionInfo(String pid, Long majorVersion, Long minorVersion) {
            this.pid = pid;
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
        }

        public String getPid() {
            return pid;
        }

        public Long getMajorVersion() {
            return majorVersion;
        }

        public Long getMinorVersion() {
            return minorVersion;
        }
    }

    @Option(names = { "--after" }, description = "Filter on dataset versions published after this timestamp (ISO 8601 format)")
    private OffsetDateTime after = OffsetDateTime.parse("1970-01-01T00:00:00Z");

    @Option(names = { "--archived" }, description = "Filter on archived dataset versions")
    private boolean archived;

    @Option(names = { "--unarchived" }, description = "Filter on unarchived dataset versions")
    private boolean unarchived;

    @Option(names = { "--updatecurrent" }, description = "An updatecurrent action was performed on the dataset version")
    private boolean updateCurrent;

    public DatasetsGetPublished(DatabaseApi dbApi) {
        this.dbApi = dbApi;
    }

    @Override
    protected Integer doCall() throws Exception {
        String query = """
            SELECT dvo.protocol || ':' || dvo.authority || '/' || dvo.identifier AS PID,
                   dsv.versionnumber                                             AS MAJORVERSION,
                   dsv.minorversionnumber                                        AS MINORVERSION
            FROM datasetversion dsv
                     JOIN dvobject dvo ON dsv.dataset_id = dvo.id
            WHERE dsv.lastupdatetime > ?
              AND dsv.versionstate IN ('RELEASED', 'DEACCESSIONED')
              AND ((? = true -- archived
                AND dsv.archivalcopylocation IS NOT NULL) -- archival location set
                OR
                   (? = true -- unarchived
                       AND dsv.archivalcopylocation IS NULL) -- archival location not set
                OR
                   (? = false AND ? = false))
              AND (? = true -- updateCurrent
                       AND dsv.lastupdatetime > dsv.releasetime -- last update after release time
                OR (? = false)) -- not filtering on updateCurrent
            ORDER BY PID ASC,
                     MAJORVERSION ASC,
                     MINORVERSION ASC;            
            """;

        Object[] parameters = new Object[] {
            java.sql.Timestamp.from(after.toInstant()),
            archived,
            unarchived,
            archived,
            unarchived,
            updateCurrent,
            updateCurrent
        };

        try (var context = dbApi.query(query, (java.sql.ResultSet rs) -> {
            try {
                return new DatasetVersionInfo(
                    rs.getString("PID"),
                    rs.getObject("MAJORVERSION", Long.class),
                    rs.getObject("MINORVERSION", Long.class)
                );
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to map ResultSet row to DatasetVersionInfo", e);
            }
        })) {
            java.util.List<DatasetVersionInfo> results = context.executeFor(java.util.Collections.singletonList(parameters));

            System.out.printf("%-40s %-15s%n", "PID", "Version");
            System.out.println("-".repeat(56));
            for (DatasetVersionInfo info : results) {
                String version = info.getMajorVersion() + "." + info.getMinorVersion();
                System.out.printf("%-40s %-15s%n", info.getPid(), version);
            }
        }

        return 0;
    }
}
