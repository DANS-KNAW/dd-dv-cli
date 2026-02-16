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

import io.dropwizard.util.DataSize;
import io.dropwizard.util.DataSizeUnit;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.dataverse.DatabaseApi;
import nl.knaw.dans.lib.util.DataSizeUnitConverter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
    name = "datasets-get-storage-size",
    description = "Generate a CSV file with dataset storage sizes and file counts",
    mixinStandardHelpOptions = true
)
@RequiredArgsConstructor
public class DatasetsGetStorageSize extends AbstractDatabaseCmd implements Callable<Integer> {

    @Data
    public static class DatasetStorageInfo {
        private final String pid;
        private final long storage;
        private final long files;
    }

    private final DatabaseApi dbApi;

    @Option(names = { "-b", "--base" }, converter = DataSizeUnitConverter.class, description = "Base for storage size: B, KB, MB, GB, TB, PB, KiB, MiB, GiB, TiB, PiB (case-insensitive).")
    private DataSizeUnit base;

    @Option(names = { "--min-size", }, description = "Minimum storage size (e.g., 100M, 1G)")
    private DataSize minSize;

    @Option(names = { "--min-files" }, description = "Minimum number of files", defaultValue = "0")
    private Long minFiles;

    @Option(names = { "--max-size" }, description = "Maximum storage size (e.g., 100M, 1G)")
    private DataSize maxSize;

    @Option(names = { "--max-files" }, description = "Maximum number of files")
    private Long maxFiles;

    @Option(names = { "-o", "--output-file" }, defaultValue = "-", description = "Output CSV file (default: stdout)")
    private String outputFile;

    @Override
    protected Integer doCall() throws Exception {
        List<DatasetStorageInfo> results = fetchResults();

        if (!"-".equals(outputFile) && !outputFile.toLowerCase().endsWith(".csv")) {
            outputFile += ".csv";
        }

        try (PrintWriter out = createOutputWriter();
            CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.builder()
                .setHeader("PID", "STORAGE", "FILES")
                .build())) {
            for (DatasetStorageInfo info : results) {
                Object storageOutput;

                if (base != null) {
                    storageOutput = String.format("%.1f", (double) info.getStorage() / base.toBytes(1));
                }
                else {
                    storageOutput = info.getStorage();
                }

                printer.printRecord(info.getPid(), storageOutput, info.getFiles());
            }
            printer.flush();
        }

        return 0;
    }

    private PrintWriter createOutputWriter() throws Exception {
        if ("-".equals(outputFile)) {
            return new PrintWriter(System.out, true);
        }
        else {
            return new PrintWriter(outputFile);
        }
    }

    private List<DatasetStorageInfo> fetchResults() throws Exception {
        // Since we want to count each datafile only once across versions for each dataset,
        // we use a subquery to find unique (dataset, datafile) pairs first.
        String query = """
            SELECT PID, SUM(filesize) AS STORAGE, COUNT(datafile_id) AS FILES
            FROM (
                SELECT DISTINCT dvo.protocol || ':' || dvo.authority || '/' || dvo.identifier AS PID,
                                df.id AS datafile_id,
                                df.filesize
                FROM dataset ds
                         JOIN dvobject dvo ON ds.id = dvo.id
                         JOIN datasetversion dsv ON ds.id = dsv.dataset_id
                         LEFT JOIN filemetadata fmd ON dsv.id = fmd.datasetversion_id
                         LEFT JOIN datafile df ON fmd.datafile_id = df.id
            ) AS unique_files
            GROUP BY PID
            HAVING ((SUM(filesize) IS NULL AND 0 = ?) OR (SUM(filesize) IS NOT NULL AND SUM(filesize) >= ?))
               AND (COUNT(datafile_id) >= ?)
               AND ((SUM(filesize) IS NULL) OR (SUM(filesize) IS NOT NULL AND SUM(filesize) <= ?))
               AND (COUNT(datafile_id) <= ?)
            ORDER BY PID ASC;
            """;

        long minSizeBytes = minSize != null ? minSize.toBytes() : 0L;
        long maxSizeBytes = maxSize != null ? maxSize.toBytes() : Long.MAX_VALUE;
        long maxFilesNum = maxFiles != null ? maxFiles : Long.MAX_VALUE;

        Object[] parameters = new Object[] {
            minSizeBytes, minSizeBytes,
            minFiles,
            maxSizeBytes,
            maxFilesNum
        };

        try (var context = dbApi.query(query, (ResultSet rs) -> {
            try {
                return new DatasetStorageInfo(
                    rs.getString("PID"),
                    rs.getLong("STORAGE"),
                    rs.getLong("FILES")
                );
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to map ResultSet row to DatasetStorageInfo", e);
            }
        })) {
            List<Object[]> paramList = new ArrayList<>();
            paramList.add(parameters);
            return context.executeFor(paramList);
        }
    }
}
