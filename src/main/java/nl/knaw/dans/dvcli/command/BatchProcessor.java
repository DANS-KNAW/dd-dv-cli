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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class BatchProcessor implements Closeable {
    public interface RowHandler {
        Result handle(Row row) throws Exception;
    }

    public enum Status {
        OK,
        FAILED,
        SKIPPED
    }

    public static class Result {
        private final Status status;
        private final String message;

        private Result(Status status, String message) {
            this.status = status;
            this.message = message;
        }

        public static Result ok(String message) {
            return new Result(Status.OK, message);
        }

        public static Result failed(String message) {
            return new Result(Status.FAILED, message);
        }

        public static Result skipped(String message) {
            return new Result(Status.SKIPPED, message);
        }

        public Status getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }
    }

    public static class Summary {
        private final int okCount;
        private final int failedCount;
        private final int skippedCount;

        public Summary(int okCount, int failedCount, int skippedCount) {
            this.okCount = okCount;
            this.failedCount = failedCount;
            this.skippedCount = skippedCount;
        }

        public int getOkCount() {
            return okCount;
        }

        public int getFailedCount() {
            return failedCount;
        }

        public int getSkippedCount() {
            return skippedCount;
        }
    }

    public static class Row {
        private final long rowNumber;
        private final List<String> headers;
        private final Map<String, String> values;

        public Row(long rowNumber, List<String> headers, Map<String, String> values) {
            this.rowNumber = rowNumber;
            this.headers = List.copyOf(headers);
            this.values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        }

        public long getRowNumber() {
            return rowNumber;
        }

        public List<String> getHeaders() {
            return headers;
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
    private final List<Map<String, String>> rows;

    private BatchProcessor(List<String> headers, List<Map<String, String>> rows, Writer writer) throws IOException {
        this.headers = List.copyOf(headers);
        this.rows = List.copyOf(rows);

        var reportHeaders = new ArrayList<>(headers);
        reportHeaders.add("result");
        reportHeaders.add("message");
        this.printer = new CSVPrinter(writer, CSVFormat.DEFAULT.builder()
            .setHeader(reportHeaders.toArray(String[]::new))
            .get());
    }

    public static BatchProcessor forCsv(Path inputFile, Writer writer) throws IOException {
        try (var reader = Files.newBufferedReader(inputFile);
            CSVParser parser = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setTrim(true)
                .get()
                .parse(reader)) {
            var headers = parser.getHeaderNames();
            var rows = new ArrayList<Map<String, String>>();
            for (CSVRecord record : parser) {
                var values = new LinkedHashMap<String, String>();
                for (String header : headers) {
                    values.put(header, record.get(header));
                }
                rows.add(values);
            }
            return new BatchProcessor(headers, rows, writer);
        }
    }

    public static BatchProcessor forSingleRow(Map<String, String> row, Writer writer) throws IOException {
        var headers = new ArrayList<>(row.keySet());
        return new BatchProcessor(headers, List.of(new LinkedHashMap<>(row)), writer);
    }

    public Summary process(RowHandler handler) throws IOException {
        int okCount = 0;
        int failedCount = 0;
        int skippedCount = 0;

        for (int i = 0; i < rows.size(); i++) {
            var row = new Row(i + 1L, headers, rows.get(i));
            Result result;

            try {
                result = handler.handle(row);
            }
            catch (Exception e) {
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
        }

        return new Summary(okCount, failedCount, skippedCount);
    }

    @Override
    public void close() throws IOException {
        printer.flush();
    }
}
