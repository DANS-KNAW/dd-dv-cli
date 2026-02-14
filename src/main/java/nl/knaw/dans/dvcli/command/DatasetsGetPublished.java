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

import lombok.Data;
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.dataverse.DatabaseApi;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.PrintWriter;
import java.time.OffsetDateTime;
import java.util.concurrent.Callable;

@Command(
    name = "datasets-get-published",
    description = "Get published datasets based on various filters",
    mixinStandardHelpOptions = true
)
@RequiredArgsConstructor
public class DatasetsGetPublished extends AbstractDatabaseCmd implements Callable<Integer> {
    @Data
    public static class DatasetVersionInfo {
        private final String pid;
        private final Long majorVersion;
        private final Long minorVersion;
    }

    public static class CsvOptions {
        @Option(names = { "--csv", "-c" }, required = true, description = "Format output as CSV")
        private boolean csv;

        @Option(names = { "--output", "-o" }, description = "Output file (default: stdout)")
        private java.io.File outputFile;
    }

    private final DatabaseApi dbApi;

    @ArgGroup(exclusive = false, heading = "CSV options:%n")
    private CsvOptions csvOptions;

    @Option(names = { "--after" }, description = "Filter on dataset versions published after this timestamp (ISO 8601 format)")
    private OffsetDateTime after = OffsetDateTime.parse("1970-01-01T00:00:00Z");

    @Option(names = { "--archived" }, description = "Filter on archived dataset versions")
    private boolean archived;

    @Option(names = { "--unarchived" }, description = "Filter on unarchived dataset versions")
    private boolean unarchived;

    @Option(names = { "--updatecurrent" }, description = "An updatecurrent action was performed on the dataset version")
    private boolean updateCurrent;

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
            java.io.File outputFile = csvOptions != null ? csvOptions.outputFile : null;
            boolean csv = csvOptions != null && csvOptions.csv;

            try (var out = outputFile != null ? new PrintWriter(outputFile) : new PrintWriter(System.out, true)) {
                if (csv) {
                    try (var printer = new CSVPrinter(out, CSVFormat.DEFAULT.builder()
                        .setHeader("PID", "MAJORVERSION", "MINORVERSION")
                        .build())) {
                        for (DatasetVersionInfo info : results) {
                            printer.printRecord(info.getPid(), info.getMajorVersion(), info.getMinorVersion());
                            printer.flush();
                        }
                    }
                }
                else {
                    out.printf("%-40s %-15s%n", "PID", "Version");
                    out.println("-".repeat(56));
                    for (DatasetVersionInfo info : results) {
                        String version = info.getMajorVersion() + "." + info.getMinorVersion();
                        out.printf("%-40s %-15s%n", info.getPid(), version);
                    }
                }
            }
        }

        return 0;
    }
}
