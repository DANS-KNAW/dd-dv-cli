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
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hc.core5.http.HttpStatus;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

@Command(
    name = "dataset-archive-version",
    description = "Archives a specific dataset version or multiple versions from a CSV file",
    mixinStandardHelpOptions = true
)
@Slf4j
public class DatasetArchiveVersion extends AbstractDatabaseCmd implements Callable<Integer> {
    private final DataverseClient dataverseClient;

    public DatasetArchiveVersion(DataverseClient dataverseClient) {
        this.dataverseClient = dataverseClient;
    }

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

    @Option(names = "--force", description = "Force re-archiving of already archived versions")
    private boolean force;

    @Option(names = "--report", description = "Basename of the report containing skipped PIDs", required = true)
    private String reportBasename;

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

        Set<String> skippedPids = new HashSet<>();
        int successCount = 0;
        int failCount = 0;

        for (DatasetVersionKey key : versionsToArchive) {
            if (skippedPids.contains(key.getPid())) {
                log.info("Skipping {} {} because a previous version failed or was not archived", key.getPid(), key.getVersionString());
                continue;
            }

            try {
                processVersion(key);
                successCount++;
            }
            catch (Exception e) {
                log.error("Failed to archive {} {}: {}", key.getPid(), key.getVersionString(), e.getMessage());
                skippedPids.add(key.getPid());
                failCount++;
            }
        }

        if (reportBasename != null && !skippedPids.isEmpty()) {
            writeReport(skippedPids);
        }

        log.info("Finished: {} succeeded, {} failed/skipped", successCount, failCount);
        return failCount == 0 ? 0 : 1;
    }

    private List<DatasetVersionKey> readCsv(File file) throws IOException {
        List<DatasetVersionKey> result = new ArrayList<>();
        try (CSVParser parser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT.builder().setHeader().setTrim(true).build())) {
            for (CSVRecord record : parser) {
                String pid = record.get("PID");
                int major = Integer.parseInt(record.get("MAJORVERSION"));
                int minor = Integer.parseInt(record.get("MINORVERSION"));
                result.add(new DatasetVersionKey(pid, major, minor));
            }
        }
        return result;
    }

    private void processVersion(DatasetVersionKey key) throws IOException, DataverseException {
        log.info("Processing {} version {}", key.getPid(), key.getVersionString());


        List<DatasetVersion> versions = dataverseClient.dataset(key.getPid()).getAllVersions().getData();
        versions.sort(Comparator.comparing(DatasetVersion::getVersionNumber).thenComparing(DatasetVersion::getVersionMinorNumber));

        for (DatasetVersion v : versions) {
            if (v.getVersionNumber() < key.getMajor() || (v.getVersionNumber() == key.getMajor() && v.getVersionMinorNumber() < key.getMinor())) {
                // Check archival status of this preceding version
                String vStr = v.getVersionNumber() + "." + v.getVersionMinorNumber();
                try {
                    var status = dataverseClient.dataset(key.getPid()).getArchivalStatus(vStr).getData();
                    if (!"archived".equalsIgnoreCase(status.getStatus())) {
                        throw new IllegalStateException("Preceding version " + vStr + " is not archived (status: " + status.getStatus() + ")");
                    }
                }
                catch (DataverseException e) {
                    if (e.getStatus() == HttpStatus.SC_NOT_FOUND) {
                        throw new IllegalStateException("Preceding version " + vStr + " has no archival status");
                    }
                    throw e;
                }
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

        // 3. Archiveer de versie
        dataverseClient.admin().submitDatasetVersionToArchive(key.getPid(), key.getVersionString(), true);
        log.info("Submitted {} version {} to archive", key.getPid(), key.getVersionString());
    }

    private void writeReport(Set<String> skippedPids) throws IOException {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        File reportFile = new File(reportBasename + "-" + timestamp + ".txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(reportFile))) {
            for (String pid : skippedPids) {
                writer.write(pid);
                writer.newLine();
            }
        }
        log.info("Report written to {}", reportFile.getAbsolutePath());
    }
}
