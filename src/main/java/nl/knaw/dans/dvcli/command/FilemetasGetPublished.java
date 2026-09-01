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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
    name = "filemetas-get-published",
    description = "Get filemetadata records from the latest published version of each dataset",
    mixinStandardHelpOptions = true
)
@RequiredArgsConstructor
public class FilemetasGetPublished extends AbstractDatabaseCmd implements Callable<Integer> {

    @Data
    public static class FileMetadataInfo {
        private final Long fileId;
        private final String datasetPid;
        private final String label;
        private final String directoryLabel;
    }

    private final DatabaseApi dbApi;

    @Option(names = { "--output", "-o" }, description = "Output file", required = true)
    private File outputFile;

    @Option(names = { "--dataset-pid" }, description = "Include dataset PID in output")
    private boolean datasetPid;

    @Option(names = { "--label" }, description = "Include file label in output")
    private boolean label;

    @Option(names = { "--directory-label" }, description = "Include directory label in output")
    private boolean directoryLabel;

    @Override
    protected Integer doCall() throws Exception {
        List<FileMetadataInfo> results = fetchResults();

        if (!outputFile.getName().toLowerCase().endsWith(".csv")) {
            File requestedOutputFile = outputFile;
            outputFile = new File(outputFile.getParentFile(), outputFile.getName() + ".csv");
            System.err.printf("Output file '%s' does not end with .csv; writing to '%s'%n", requestedOutputFile, outputFile);
        }

        try (var out = new PrintWriter(outputFile)) {
            writeCsvFile(results, out);
        }

        return 0;
    }

    private List<FileMetadataInfo> fetchResults() throws Exception {
        String query = """
            WITH latest_published AS (
                SELECT DISTINCT ON (dv.dataset_id)
                       dv.id AS DATASETVERSION_ID,
                       dv.dataset_id
                FROM datasetversion dv
                WHERE dv.versionstate = 'RELEASED'
                ORDER BY dv.dataset_id,
                         dv.versionnumber DESC,
                         dv.minorversionnumber DESC
            )
            SELECT ds.protocol || ':' || ds.authority || '/' || ds.identifier AS DATASET_PID,
                   fm.datafile_id                                             AS FILEID,
                   fm.label                                                   AS LABEL,
                   fm.directorylabel                                          AS DIRECTORY_LABEL
            FROM latest_published lp
                     JOIN filemetadata fm ON fm.datasetversion_id = lp.datasetversion_id
                     JOIN dvobject ds ON ds.id = lp.dataset_id
            ORDER BY DATASET_PID, DIRECTORY_LABEL, LABEL, FILEID;
            """;

        try (var context = dbApi.query(query, (ResultSet rs) -> {
            try {
                return new FileMetadataInfo(
                    rs.getLong("FILEID"),
                    rs.getString("DATASET_PID"),
                    rs.getString("LABEL"),
                    rs.getString("DIRECTORY_LABEL")
                );
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to map ResultSet row to FileMetadataInfo", e);
            }
        })) {
            return context.executeFor(Collections.singletonList(new Object[0]));
        }
    }

    private void writeCsvFile(List<FileMetadataInfo> results, PrintWriter out) throws Exception {
        List<String> headers = new ArrayList<>();
        headers.add("FILEID");
        if (datasetPid) {
            headers.add("DATASET_PID");
        }
        if (label) {
            headers.add("LABEL");
        }
        if (directoryLabel) {
            headers.add("DIRECTORY_LABEL");
        }

        try (var printer = new CSVPrinter(out, CSVFormat.DEFAULT.builder()
            .setHeader(headers.toArray(new String[0]))
            .get())) {
            for (FileMetadataInfo info : results) {
                List<Object> record = new ArrayList<>();
                record.add(info.getFileId());
                if (datasetPid) {
                    record.add(info.getDatasetPid());
                }
                if (label) {
                    record.add(info.getLabel());
                }
                if (directoryLabel) {
                    record.add(info.getDirectoryLabel());
                }
                printer.printRecord(record);
            }
            printer.flush();
        }
    }
}
