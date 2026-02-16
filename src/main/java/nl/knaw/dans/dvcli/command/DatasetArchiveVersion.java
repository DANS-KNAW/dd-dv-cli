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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DatabaseApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.hc.core5.http.HttpStatus;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

@Command(
    name = "dataset-archive-version",
    description = "Archives a specific dataset version or multiple dataset versions from a CSV file",
    mixinStandardHelpOptions = true
)
@Slf4j
@RequiredArgsConstructor
public class DatasetArchiveVersion extends AbstractDatabaseCmd implements Callable<Integer> {
    private final DataverseClient dataverseClient;
    private final DatabaseApi dbApi;

    @ArgGroup(multiplicity = "1")
    private ExclusiveOptions exclusiveOptions;

    static class ExclusiveOptions {
        @ArgGroup(exclusive = false)
        SingleVersionOptions singleVersion;

        @Option(names = { "-i", "--input-file" }, description = "Input CSV file (PID, MAJORVERSION, MINORVERSION)")
        File inputFile;
    }

    static class SingleVersionOptions {
        @Option(names = { "-p", "--pid" }, required = true, description = "Persistent Identifier of the dataset")
        String pid;

        @Option(names = { "-v", "--version" }, required = true, description = "Version of the dataset (e.g., 1.2)")
        String version;
    }

    @Option(names = { "--skip-pids-from", "-s" }, description = "Skip PIDs from the PID-column in the given CSV file")
    private File skipPidsFrom;

    @Option(names = { "--force", "-f" }, description = "Force re-archiving of already archived versions")
    private boolean force;

    @Option(names = { "--report", "-r" }, description = "Basename of the report containing skipped PIDs", required = true)
    private String reportBasename;

    @Option(names = { "-w", "--wait-between-items" }, description = "Wait period (in seconds) between items", defaultValue = "3")
    private int waitBetweenItems;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class DatasetVersionKey {
        private String pid;
        private int major;
        private int minor;

        public String getVersionString() {
            return major + "." + minor;
        }
    }

    @Data
    @AllArgsConstructor
    private static class InternalVersionInfo {
        private int major;
        private int minor;
        private boolean archived;
    }

    @Data
    @AllArgsConstructor
    private static class ReportRecord {
        private String pid;
        private int major;
        private int minor;
        private String result;
        private String message;
    }

    @Override
    public Integer doCall() throws Exception {
        List<DatasetVersionKey> versionsToArchive = new ArrayList<>();

        if (exclusiveOptions.inputFile != null) {
            versionsToArchive.addAll(readCsv(exclusiveOptions.inputFile));
        }
        else {
            String[] versionParts = exclusiveOptions.singleVersion.version.split("\\.");
            int major = Integer.parseInt(versionParts[0]);
            int minor = versionParts.length > 1 ? Integer.parseInt(versionParts[1]) : 0;
            versionsToArchive.add(new DatasetVersionKey(exclusiveOptions.singleVersion.pid, major, minor));
        }

        List<ReportRecord> reportRecords = new ArrayList<>();
        List<String> skippedPids = new ArrayList<>();
        List<String> pidsToSkipFromFile = new ArrayList<>();

        if (skipPidsFrom != null) {
            pidsToSkipFromFile = readPidsToSkip(skipPidsFrom);
            skippedPids.addAll(pidsToSkipFromFile);
        }

        int successCount = 0;
        int failCount = 0;

        for (DatasetVersionKey key : versionsToArchive) {
            if (skippedPids.contains(key.getPid())) {
                if (pidsToSkipFromFile.contains(key.getPid())) {
                    log.info("Skipping {} {} because it is in the skip-list", key.getPid(), key.getVersionString());
                    reportRecords.add(new ReportRecord(key.getPid(), key.getMajor(), key.getMinor(), "SKIPPED", "Skipped because it is in the skip-list"));
                }
                else {
                    log.info("Skipping {} {} because a previous version failed or was not archived", key.getPid(), key.getVersionString());
                    reportRecords.add(new ReportRecord(key.getPid(), key.getMajor(), key.getMinor(), "ERROR", "Skipped because a previous version of this PID failed or was not archived"));
                    failCount++;
                }
                continue;
            }

            if (waitBetweenItems > 0 && (successCount > 0 || failCount > 0)) {
                log.info("Waiting {} seconds before processing next item...", waitBetweenItems);
                Thread.sleep(waitBetweenItems * 1000L);
            }

            try {
                processVersion(key);
                reportRecords.add(new ReportRecord(key.getPid(), key.getMajor(), key.getMinor(), "OK", "Version archived successfully"));
                successCount++;
            }
            catch (Exception e) {
                log.error("Failed to archive {} {}: {}", key.getPid(), key.getVersionString(), e.getMessage());
                skippedPids.add(key.getPid());
                reportRecords.add(new ReportRecord(key.getPid(), key.getMajor(), key.getMinor(), "ERROR", e.getMessage()));
                failCount++;
            }
        }

        if (reportBasename != null) {
            writeReport(reportRecords);
        }

        log.info("Finished: {} succeeded, {} failed/skipped", successCount, failCount);
        return failCount == 0 ? 0 : 1;
    }

    private List<DatasetVersionKey> readCsv(File file) throws IOException {
        List<DatasetVersionKey> result = new ArrayList<>();
        try (var parser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setTrim(true).build())) {
            for (CSVRecord record : parser) {
                String pid = record.get("PID");
                int major = Integer.parseInt(record.get("MAJORVERSION"));
                int minor = Integer.parseInt(record.get("MINORVERSION"));
                result.add(new DatasetVersionKey(pid, major, minor));
            }
        }
        return result;
    }

    private List<String> readPidsToSkip(File file) throws IOException {
        List<String> result = new ArrayList<>();
        try (var parser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setTrim(true).build())) {
            for (CSVRecord record : parser) {
                result.add(record.get("PID"));
            }
        }
        return result;
    }

    private void processVersion(DatasetVersionKey key) throws Exception {
        log.info("Processing {} version {}", key.getPid(), key.getVersionString());

        List<InternalVersionInfo> versions = fetchReleasedAndDeaccessionedVersionsFromDb(key.getPid());

        for (InternalVersionInfo v : versions) {
            if (isDatasetVersionKeyPrecededBy(key, v) && !v.isArchived()) {
                String vStr = v.getMajor() + "." + v.getMinor();
                throw new IllegalStateException("Preceding version " + vStr + " is not archived");
            }
            if (isDatasetVersionKeyFollowedBy(key, v) && v.isArchived()) {
                String vStr = v.getMajor() + "." + v.getMinor();
                throw new IllegalStateException("Succeeding version " + vStr + " is already archived; refusing to archive " + key.getVersionString());
            }
        }

        if (force) {
            try {
                dataverseClient.dataset(key.getPid()).deleteArchivalStatus(key.getVersionString());
                log.info("Deleted archival status for {} version {} (force=true)", key.getPid(), key.getVersionString());
            }
            catch (DataverseException e) {
                if (e.getStatus() != HttpStatus.SC_NOT_FOUND) {
                    throw e;
                }
            }
        }

        dataverseClient.admin().submitDatasetVersionToArchive(key.getPid(), key.getVersionString(), true);
        log.info("Submitted {} version {} to archive", key.getPid(), key.getVersionString());

        waitForArchivalToFinish(key);
    }

    private boolean isDatasetVersionKeyPrecededBy(DatasetVersionKey key, InternalVersionInfo v) {
        return v.getMajor() < key.getMajor() || (v.getMajor() == key.getMajor() && v.getMinor() < key.getMinor());
    }

    private boolean isDatasetVersionKeyFollowedBy(DatasetVersionKey key, InternalVersionInfo v) {
        return v.getMajor() > key.getMajor() || (v.getMajor() == key.getMajor() && v.getMinor() > key.getMinor());
    }

    private void waitForArchivalToFinish(DatasetVersionKey key) throws Exception {
        log.info("Waiting for archival of {} version {} to finish...", key.getPid(), key.getVersionString());
        var startTime = System.currentTimeMillis();
        var timeout = 600_000L; // 10 minutes timeout for polling
        var interval = 5000L; // 5 seconds polling interval

        while (!isArchived(key)) {
            if (System.currentTimeMillis() - startTime > timeout) {
                throw new RuntimeException("Timeout waiting for archival to finish");
            }

            Thread.sleep(interval);
        }
    }

    private boolean isArchived(DatasetVersionKey key) throws IOException {
        try {
            var response = dataverseClient.dataset(key.getPid()).getArchivalStatus(key.getVersionString());
            return response.getHttpResponse().getCode() == HttpStatus.SC_OK;
        }
        catch (DataverseException e) {
            String message = e.getMessage();
            if (e.getStatus() == HttpStatus.SC_NOT_FOUND
                && message != null
                && message.contains("This dataset version has not been archived")) {
                return false; // Not archived yet
            }
            else {
                throw new RuntimeException("Failed to check archival status for " + key.getPid() + " version " + key.getVersionString(), e);
            }
        }
    }

    private List<InternalVersionInfo> fetchReleasedAndDeaccessionedVersionsFromDb(String pid) throws Exception {
        String query = """
            SELECT dsv.versionnumber      AS MAJORVERSION,
                   dsv.minorversionnumber AS MINORVERSION,
                   dsv.archivalcopylocation
            FROM datasetversion dsv
                     JOIN dvobject dvo ON dsv.dataset_id = dvo.id
            WHERE dvo.protocol || ':' || dvo.authority || '/' || dvo.identifier = ?
                 AND dsv.versionstate IN ('RELEASED', 'DEACCESSIONED')
            ORDER BY MAJORVERSION ASC, MINORVERSION ASC
            """;

        try (var context = dbApi.query(query, (ResultSet rs) -> {
            try {
                return new InternalVersionInfo(
                    rs.getInt("MAJORVERSION"),
                    rs.getInt("MINORVERSION"),
                    rs.getString("archivalcopylocation") != null
                );
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to map ResultSet row to InternalVersionInfo", e);
            }
        })) {
            return context.executeFor(Collections.singletonList(new Object[] { pid }));
        }
    }

    private void writeReport(List<ReportRecord> records) throws IOException {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss"));
        File reportFile = new File(reportBasename + "-" + timestamp + ".csv");
        try (PrintWriter out = new PrintWriter(new FileWriter(reportFile));
            CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.builder()
                .setHeader("PID", "MAJORVERSION", "MINORVERSION", "RESULT", "MESSAGE")
                .build())) {
            for (ReportRecord record : records) {
                printer.printRecord(record.getPid(), record.getMajor(), record.getMinor(), record.getResult(), record.getMessage());
            }
            printer.flush();
        }
        log.info("Report written to {}", reportFile.getAbsolutePath());
    }
}
