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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class BatchProcessor implements Closeable {
    private static final List<String> REPORT_COLUMNS = List.of("result", "message");

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

        public Row(long rowNumber, List<String> headers, Map<String, String> values) {
            this.rowNumber = rowNumber;
            this.headers = List.copyOf(headers);
            this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        }

        public boolean hasColumn(String name) {
            return values.containsKey(name);
        }

        public String getValue(String name) {
            return values.get(name);
        }

        public Map<String, String> asMap() {
            return values;
        }
    }

    private final CSVPrinter printer;
    private final List<String> headers;
    private final Iterator<Map<String, String>> rows;
    private final Closeable closeable;

    private BatchProcessor(List<String> headers, Iterator<Map<String, String>> rows, Closeable closeable, Writer writer) throws IOException {
        for (String reportColumn : REPORT_COLUMNS) {
            if (headers.contains(reportColumn)) {
                throw new IllegalArgumentException("Input contains reserved column: " + reportColumn);
            }
        }

        this.headers = List.copyOf(headers);
        this.rows = rows;
        this.closeable = closeable;

        var reportHeaders = new ArrayList<>(headers);
        reportHeaders.addAll(REPORT_COLUMNS);
        this.printer = new CSVPrinter(writer, CSVFormat.DEFAULT.builder()
            .setHeader(reportHeaders.toArray(String[]::new))
            .get());
        log.debug("Initialized batch processor with headers {}", this.headers);
    }

    public static BatchProcessor forCsv(Path inputFile, Writer writer) throws IOException {
        var reader = Files.newBufferedReader(inputFile);
        try {
            CSVParser parser = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
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
            }, writer);
        }
        catch (Exception e) {
            reader.close();
            throw e;
        }
    }

    public static BatchProcessor forSingleRow(Map<String, String> row, Writer writer) throws IOException {
        var headers = new ArrayList<>(row.keySet());
        List<Map<String, String>> rows = List.of(new LinkedHashMap<>(row));
        return new BatchProcessor(headers, rows.iterator(), () -> {
        }, writer);
    }

    public Summary process(RowHandler handler) throws IOException {
        int okCount = 0;
        int failedCount = 0;
        int skippedCount = 0;

        long rowNumber = 1;
        while (rows.hasNext()) {
            var row = new Row(rowNumber++, headers, rows.next());
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
