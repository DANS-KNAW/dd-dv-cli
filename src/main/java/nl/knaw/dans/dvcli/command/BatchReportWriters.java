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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Creates the writer for the report of a (batch) run: a timestamped CSV file in a reports directory, or stdout.
 */
@Slf4j
final class BatchReportWriters {
    private static final DateTimeFormatter REPORT_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");

    private BatchReportWriters() {
    }

    /**
     * @param inputFile         the batch input file, or null in single-row mode (report goes to stdout)
     * @param reportsDir        reports directory given on the command line, or null
     * @param defaultReportsDir configured reports directory, used if reportsDir is null; if both are null the report goes to stdout
     */
    static Writer create(Path inputFile, Path reportsDir, Path defaultReportsDir) throws IOException {
        if (inputFile == null) {
            return stdoutWriter();
        }
        var effectiveReportsDir = reportsDir != null ? reportsDir : defaultReportsDir;
        if (effectiveReportsDir == null) {
            log.info("No reports directory configured; writing batch report to stdout");
            return stdoutWriter();
        }
        Files.createDirectories(effectiveReportsDir);
        var reportFile = effectiveReportsDir.resolve(createReportFileName(inputFile));
        log.info("Writing batch report to {}", reportFile);
        return Files.newBufferedWriter(reportFile, StandardCharsets.UTF_8);
    }

    private static Writer stdoutWriter() {
        return new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
    }

    private static String createReportFileName(Path csvInputFile) {
        var fileName = csvInputFile.getFileName().toString();
        var dotIndex = fileName.lastIndexOf('.');
        var basename = dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
        return basename + "-" + LocalDateTime.now().format(REPORT_TIMESTAMP_FORMAT) + ".csv";
    }
}
