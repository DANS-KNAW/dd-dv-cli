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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Processes input rows as a batch and writes a CSV report containing the original input columns followed by processing result columns.
 * <p>
 * Rows can be supplied from a CSV file or from a single map-backed row. Each row is exposed to a {@link RowHandler}, which returns a {@link Result} indicating whether the row was processed
 * successfully, failed, or skipped. Exceptions thrown by the handler are converted into failed results for the affected row.
 * <p>
 * The generated report appends the reserved columns {@code result} and {@code message}. Input headers must not contain columns with those names, compared case-insensitively.
 * <p>
 * Processing is sequential. A summary is returned after all rows have been handled, containing counts for each result status. Report records are flushed after each processed row.
 * <p>
 * Instances own the report printer and any input resources opened by the factory method. Call {@link #close()} when processing is complete to release resources.
 */
@Slf4j
public class BatchProcessor implements Closeable {

    public interface RowHandler {
        Result handle(Row row) throws Exception;
    }

    public enum Status {
        OK,
        FAILED,
        SKIPPED
    }

    @Getter
    @RequiredArgsConstructor
    public static class Result {
        private final Status status;
        private final String message;

        public static Result ok(String message) {
            return new Result(Status.OK, message);
        }

        public static Result failed(String message) {
            return new Result(Status.FAILED, message);
        }

        public static Result skipped(String message) {
            return new Result(Status.SKIPPED, message);
        }
    }

    @Getter
    @RequiredArgsConstructor
    public static class Summary {
        private final int okCount;
        private final int failedCount;
        private final int skippedCount;
    }

    @Getter
    public static class Row {
        private final long rowNumber;
        private final List<String> headers;
        private final Map<String, String> values;
        private final Set<String> defaultedColumns;

        public Row(long rowNumber, List<String> headers, Map<String, String> values) {
            this(rowNumber, headers, values, Set.of());
        }

        public Row(long rowNumber, List<String> headers, Map<String, String> values, Set<String> defaultedColumns) {
            this.rowNumber = rowNumber;
            this.headers = List.copyOf(headers);
            this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
            this.defaultedColumns = Set.copyOf(defaultedColumns);
        }

        /**
         * Returns whether the value of the column was filled in from the defaults, because the input had no value for it.
         */
        public boolean isDefaulted(String name) {
            return defaultedColumns.stream().anyMatch(column -> stripStar(column).equalsIgnoreCase(stripStar(name)));
        }

        public boolean hasColumn(String name) {
            return values.keySet().stream().anyMatch(name::equalsIgnoreCase);
        }

        public String getValue(String name) {
            return values.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(name))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
        }

        public Map<String, String> asMap() {
            return values;
        }
    }

    private final CSVPrinter printer;
    @Getter
    private final List<String> headers;
    private final Map<String, String> defaults;
    private final Iterator<Map<String, String>> rows;
    private final Closeable closeable;

    private BatchProcessor(List<String> headers, Iterator<Map<String, String>> rows, Closeable closeable, Writer writer, String reportColumnPrefix, Map<String, String> defaults) throws IOException {
        var reportColumns = List.of(reportColumnPrefix + "result", reportColumnPrefix + "message");
        for (String reportColumn : reportColumns) {
            if (headers.stream().anyMatch(reportColumn::equalsIgnoreCase)) {
                throw new IllegalArgumentException("Input contains reserved column: " + reportColumn);
            }
        }

        var effectiveHeaders = new ArrayList<>(headers);
        defaults.keySet().stream()
            .filter(name -> findHeader(headers, name) == null)
            .forEach(effectiveHeaders::add);
        this.headers = List.copyOf(effectiveHeaders);
        this.defaults = new LinkedHashMap<>(defaults);
        this.rows = rows;
        this.closeable = closeable;

        var reportHeaders = new ArrayList<>(this.headers);
        reportHeaders.addAll(reportColumns);
        this.printer = new CSVPrinter(writer, CSVFormat.DEFAULT.builder()
            .setHeader(reportHeaders.toArray(String[]::new))
            .get());
        log.debug("Initialized batch processor with headers {}", this.headers);
    }

    private static String stripStar(String name) {
        return name.startsWith("*") ? name.substring(1) : name;
    }

    private static String findHeader(List<String> headers, String name) {
        return headers.stream().filter(header -> stripStar(header).equalsIgnoreCase(stripStar(name))).findFirst().orElse(null);
    }

    /**
     * Returns the row values with the defaults applied: blank cells in columns that have a default get the default value and columns that are missing get the default value.
     */
    private Map<String, String> applyDefaults(Map<String, String> values, Set<String> defaulted) {
        var effective = new LinkedHashMap<>(values);
        defaults.forEach((name, defaultValue) -> {
            var header = findHeader(headers, name);
            var current = effective.get(header);
            if (current == null || current.isBlank()) {
                effective.put(header, defaultValue);
                defaulted.add(header);
            }
        });
        return effective;
    }

    public static BatchProcessor forCsv(Path inputFile, Writer writer) throws IOException {
        return forCsv(inputFile, writer, "");
    }

    /**
     * @param reportColumnPrefix prefix for the names of the report columns, e.g., "*" to distinguish them from metadata field columns
     */
    public static BatchProcessor forCsv(Path inputFile, Writer writer, String reportColumnPrefix) throws IOException {
        return forCsv(inputFile, writer, reportColumnPrefix, Map.of());
    }

    /**
     * @param defaults default values per column; they are used for blank cells and for columns missing from the input. Columns missing from the input are added to the report. A leading
     *                 star is ignored when matching column names.
     */
    public static BatchProcessor forCsv(Path inputFile, Writer writer, String reportColumnPrefix, Map<String, String> defaults) throws IOException {
        var reader = Files.newBufferedReader(inputFile);
        try {
            CSVParser parser = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setCommentMarker('#')
                .setTrim(true)
                .get()
                .parse(reader);
            var headers = parser.getHeaderNames();
            log.info("Reading batch input from {}", inputFile);
            var records = parser.iterator();
            var rows = new Iterator<Map<String, String>>() {

                @Override
                public boolean hasNext() {
                    return records.hasNext();
                }

                @Override
                public Map<String, String> next() {
                    CSVRecord record = records.next();
                    var values = new LinkedHashMap<String, String>();
                    for (String header : headers) {
                        values.put(header, record.get(header));
                    }
                    return values;
                }
            };
            return new BatchProcessor(headers, rows, () -> {
                parser.close();
                reader.close();
            }, writer, reportColumnPrefix, defaults);
        }
        catch (Exception e) {
            reader.close();
            throw e;
        }
    }

    public static BatchProcessor forSingleRow(Map<String, String> row, Writer writer) throws IOException {
        return forSingleRow(row, writer, "");
    }

    public static BatchProcessor forSingleRow(Map<String, String> row, Writer writer, String reportColumnPrefix) throws IOException {
        return forSingleRow(row, writer, reportColumnPrefix, Map.of());
    }

    /**
     * Like {@link #forSingleRow(Map, Writer, String)}, but columns that are missing from the row get the default value and are added to the report.
     */
    public static BatchProcessor forSingleRow(Map<String, String> row, Writer writer, String reportColumnPrefix, Map<String, String> defaults) throws IOException {
        var headers = new ArrayList<>(row.keySet());
        List<Map<String, String>> rows = List.of(new LinkedHashMap<>(row));
        return new BatchProcessor(headers, rows.iterator(), () -> {
        }, writer, reportColumnPrefix, defaults);
    }

    public Summary process(RowHandler handler) throws IOException {
        return process(handler, 0);
    }

    public Summary process(RowHandler handler, long waitBetweenRowsMillis) throws IOException {
        int okCount = 0;
        int failedCount = 0;
        int skippedCount = 0;

        long rowNumber = 1;
        while (rows.hasNext()) {
            var defaulted = new HashSet<String>();
            var row = new Row(rowNumber++, headers, applyDefaults(rows.next(), defaulted), defaulted);
            log.debug("Processing batch row {}", row.getRowNumber());
            Result result;

            try {
                result = handler.handle(row);
            }
            catch (Exception e) {
                log.warn("Batch row {} failed: {}", row.getRowNumber(), e.getMessage());
                result = Result.failed(e.getMessage());
            }

            switch (result.getStatus()) {
                case OK -> okCount++;
                case FAILED -> failedCount++;
                case SKIPPED -> skippedCount++;
            }

            var output = new ArrayList<String>();
            for (String header : headers) {
                output.add(row.getValue(header));
            }
            output.add(result.getStatus().name());
            output.add(result.getMessage());
            printer.printRecord(output);
            printer.flush();
            log.debug("Finished batch row {} with status {}", row.getRowNumber(), result.getStatus());

            if (waitBetweenRowsMillis > 0 && rows.hasNext()) {
                try {
                    Thread.sleep(waitBetweenRowsMillis);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting between rows", e);
                }
            }
        }

        return new Summary(okCount, failedCount, skippedCount);
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        try {
            printer.close();
        }
        catch (IOException e) {
            failure = e;
        }

        try {
            closeable.close();
        }
        catch (IOException e) {
            if (failure == null) {
                failure = e;
            }
            else {
                failure.addSuppressed(e);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }
}
