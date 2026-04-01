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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.DataMessage;
import nl.knaw.dans.lib.dataverse.model.metrics.MetricsTreeNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Command(
    name = "dataverses-collect-storage-usage",
    description = "Collect the storage usage for the dataverses",
    mixinStandardHelpOptions = true
)
@RequiredArgsConstructor
@Slf4j
public class DataversesCollectStorageUsage implements Callable<Integer> {

    private final DataverseClient dataverseClient;

    @Option(names = { "-m", "--max-depth" }, defaultValue = "1", description = "The max depth of the hierarchy to traverse")
    private int maxDepth;

    @Option(names = { "-g", "--include-grand-total" }, description = "Whether to include the grand total, which almost doubles server processing time")
    private boolean includeGrandTotal;

    @Option(names = { "-o", "--output-file" }, defaultValue = "-", description = "The file to write the output to or - for stdout")
    private String outputFile;

    @Option(names = { "-f", "--format" }, defaultValue = "json", description = "Output format, one of: csv, json (default: json)")
    private String format;

    @Data
    private static class StorageUsageRow {
        private final int depth;
        private final String parentalias;
        private final String alias;
        private final String name;
        private final long storagesize;
    }

    private final Pattern sizePattern = Pattern.compile("dataverse: (.+?) bytes");

    @Override
    public Integer call() throws Exception {
        log.info("Extracting tree...");
        MetricsTreeNode treeData = dataverseClient.metrics().tree().getData();
        String alias = treeData.getAlias();
        String name = treeData.getName();
        log.info("Extracted the tree for the toplevel dataverse: {} ({})", name, alias);

        try (ResultWriter writer = createResultWriter()) {
            if (includeGrandTotal) {
                log.info("Retrieving the total size for this dataverse instance...");
                writer.writeRow(getStorageUsageRow("-", alias, name, 0));
            }

            collectChildrenSizes(treeData, maxDepth, 1, writer);
        }

        return 0;
    }

    private void collectChildrenSizes(MetricsTreeNode parentData, int maxDepth, int depth, ResultWriter writer) throws IOException, DataverseException {
        String parentAlias = parentData.getAlias();
        List<MetricsTreeNode> children = parentData.getChildren();
        if (children != null) {
            for (MetricsTreeNode child : children) {
                writer.writeRow(getStorageUsageRow(parentAlias, child.getAlias(), child.getName(), depth));
                if (depth < maxDepth) {
                    collectChildrenSizes(child, maxDepth, depth + 1, writer);
                }
            }
        }
    }

    private StorageUsageRow getStorageUsageRow(String parentAlias, String childAlias, String childName, int depth) throws IOException, DataverseException {
        log.info("Retrieving size for dataverse: {} / {} ...", parentAlias, childAlias);
        DataMessage msg = dataverseClient.dataverse(childAlias).getStorageSize().getData();
        long storageSize = extractSize(msg.getMessage());
        log.info("size: {}", storageSize);
        return new StorageUsageRow(depth, parentAlias, childAlias, childName, storageSize);
    }

    private long extractSize(String message) {
        Matcher matcher = sizePattern.matcher(message);
        if (matcher.find()) {
            String sizeStr = matcher.group(1).replaceAll("[,.]", "");
            return Long.parseLong(sizeStr);
        }
        return 0L;
    }

    private interface ResultWriter extends AutoCloseable {
        void writeRow(StorageUsageRow row) throws IOException;

        @Override
        void close() throws IOException;
    }

    private ResultWriter createResultWriter() throws IOException {
        PrintWriter out = createOutputWriter();
        if ("csv".equalsIgnoreCase(format)) {
            return new CsvResultWriter(out);
        }
        else {
            return new JsonResultWriter(out);
        }
    }

    @RequiredArgsConstructor
    private static class CsvResultWriter implements ResultWriter {
        private final PrintWriter out;
        private CSVPrinter printer;

        @Override
        public void writeRow(StorageUsageRow row) throws IOException {
            if (printer == null) {
                printer = new CSVPrinter(out, CSVFormat.DEFAULT.builder()
                    .setHeader("depth", "parentalias", "alias", "name", "storagesize")
                    .get());
            }
            printer.printRecord(row.getDepth(), row.getParentalias(), row.getAlias(), row.getName(), row.getStoragesize());
            printer.flush();
        }

        @Override
        public void close() throws IOException {
            if (printer != null) {
                printer.close();
            }
            out.close();
        }
    }

    private static class JsonResultWriter implements ResultWriter {
        private final PrintWriter out;
        private final List<StorageUsageRow> results = new ArrayList<>();

        public JsonResultWriter(PrintWriter out) {
            this.out = out;
        }

        @Override
        public void writeRow(StorageUsageRow row) {
            results.add(row);
        }

        @Override
        public void close() throws IOException {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            out.print(mapper.writeValueAsString(results));
            out.close();
        }
    }

    private PrintWriter createOutputWriter() throws IOException {
        if ("-".equals(outputFile)) {
            return new PrintWriter(System.out, true);
        }
        else {
            return new PrintWriter(outputFile);
        }
    }
}
