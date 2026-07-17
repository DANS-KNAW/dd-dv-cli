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
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
    name = "datafiles-get-published",
    description = "Get published datafiles",
    mixinStandardHelpOptions = true
)
@RequiredArgsConstructor
public class DatafilesGetPublished extends AbstractDatabaseCmd implements Callable<Integer> {

    @Data
    public static class DatafileInfo {
        private final Long fileId;
        private final String datasetPid;
        private final String checksumType;
        private final String checksumValue;
        private final Timestamp publicationTimestamp;
        private final Long filesize;
    }

    private final DatabaseApi dbApi;

    @Option(names = { "--output", "-o" }, description = "Output file", required = true)
    private File outputFile;

    @Option(names = { "--checksum-type" }, description = "Include checksum type in output")
    private boolean checksumType;

    @Option(names = { "--checksum-value" }, description = "Include checksum value in output")
    private boolean checksumValue;

    @Option(names = { "--dataset-pid" }, description = "Include dataset PID in output")
    private boolean datasetPid;

    @Option(names = { "--publication-timestamp" }, description = "Include publication timestamp in output")
    private boolean publicationTimestamp;

    @Option(names = { "--filesize" }, description = "Include filesize in output")
    private boolean filesize;

    @Option(names = { "--published" }, description = "Only include datafiles published after this timestamp (ISO-8601, e.g. 2025-01-01T00:00:00+01:00)", defaultValue = "1970-01-01T00:00:00Z")
    private OffsetDateTime publishedAfter;

    @Override
    protected Integer doCall() throws Exception {
        List<DatafileInfo> results = fetchResults();

        if (!outputFile.getName().toLowerCase().endsWith(".csv")) {
            outputFile = new File(outputFile.getParentFile(), outputFile.getName() + ".csv");
        }

        try (var out = new PrintWriter(outputFile)) {
            writeCsvFile(results, out);
        }

        return 0;
    }

    private List<DatafileInfo> fetchResults() throws Exception {
        /*
         * Note that Dataverse stores the checksum of the *original* file in the datafile table but the length of the *.tab* file (if available).
         * To also get the length of the *original* file, we have to look in the datatable table.
         */
        var publishedAfterClause = publishedAfter != null ? "AND dvo.publicationdate > ?\n" : "";
        var query = """
            SELECT dvo.id                                                      AS FILEID,
                   ds_dvo.protocol || ':' || ds_dvo.authority || '/' || ds_dvo.identifier AS DATASET_PID,
                   df.checksumtype                                             AS CHECKSUM_TYPE,
                   df.checksumvalue                                            AS CHECKSUM_VALUE,
                   dvo.publicationdate                                         AS PUBLICATION_TIMESTAMP,
                   COALESCE(dt.originalfilesize, df.filesize)                  AS FILESIZE
            FROM dvobject dvo
                     JOIN datafile df ON dvo.id = df.id
                     JOIN dvobject ds_dvo ON dvo.owner_id = ds_dvo.id
                     LEFT JOIN datatable dt ON df.id = dt.datafile_id
            WHERE dvo.dtype = 'DataFile'
              AND dvo.publicationdate IS NOT NULL
              """ + publishedAfterClause + """
            ORDER BY FILEID ASC;
            """;

        var params = publishedAfter != null
            ? new Object[] { Timestamp.from(publishedAfter.toInstant()) }
            : new Object[0];

        try (var context = dbApi.query(query, (ResultSet rs) -> {
            try {
                return new DatafileInfo(
                    rs.getLong("FILEID"),
                    rs.getString("DATASET_PID"),
                    rs.getString("CHECKSUM_TYPE"),
                    rs.getString("CHECKSUM_VALUE"),
                    rs.getTimestamp("PUBLICATION_TIMESTAMP"),
                    rs.getLong("FILESIZE")
                );
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to map ResultSet row to DatafileInfo", e);
            }
        })) {
            return context.executeFor(Collections.singletonList(params));
        }
    }

    private void writeCsvFile(List<DatafileInfo> results, PrintWriter out) throws Exception {
        List<String> headers = new ArrayList<>();
        if (datasetPid) {
            headers.add("DATASET_PID");
        }
        headers.add("FILEID");
        if (checksumType) {
            headers.add("CHECKSUM_TYPE");
        }
        if (checksumValue) {
            headers.add("CHECKSUM_VALUE");
        }
        if (publicationTimestamp) {
            headers.add("PUBLICATION_TIMESTAMP");
        }
        if (filesize) {
            headers.add("FILESIZE");
        }

        try (var printer = new CSVPrinter(out, CSVFormat.DEFAULT.builder()
            .setHeader(headers.toArray(new String[0]))
            .get())) {
            for (DatafileInfo info : results) {
                List<Object> record = new ArrayList<>();
                if (datasetPid) {
                    record.add(info.getDatasetPid());
                }
                record.add(info.getFileId());
                if (checksumType) {
                    record.add(info.getChecksumType());
                }
                if (checksumValue) {
                    record.add(info.getChecksumValue());
                }
                if (publicationTimestamp) {
                    record.add(info.getPublicationTimestamp());
                }
                if (filesize) {
                    record.add(info.getFilesize());
                }
                printer.printRecord(record);
            }
            printer.flush();
        }
    }
}
