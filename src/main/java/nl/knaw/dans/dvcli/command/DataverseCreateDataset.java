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

import io.dropwizard.util.Duration;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetFieldType;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import nl.knaw.dans.lib.dataverse.model.dataset.FieldList;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlock;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlockDefinition;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.UpdateType;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static nl.knaw.dans.dvcli.command.DatasetEditMetadata.EXPECTED_VALUE_PREFIX;
import static nl.knaw.dans.dvcli.command.DatasetEditMetadata.SPECIAL_COLUMN_PREFIX;
import static nl.knaw.dans.dvcli.command.DatasetEditMetadata.isBlank;
import static nl.knaw.dans.dvcli.command.DatasetEditMetadata.parseAssignments;
import static nl.knaw.dans.dvcli.command.DatasetEditMetadata.toFieldList;
import static nl.knaw.dans.dvcli.command.DatasetEditMetadata.toSpecialColumn;
import static nl.knaw.dans.dvcli.command.DatasetEditMetadata.trimToNull;

@Command(
    name = "dataverse-create-dataset",
    description = "Create a dataset or a batch of datasets in a collection",
    mixinStandardHelpOptions = true
)
@Slf4j
public class DataverseCreateDataset implements Callable<Integer> {
    static final String DEFAULT_ALIAS = "root";
    private static final String ALIAS = "*alias";
    private static final String PUBLISH = "*publish";
    private static final Set<String> RESERVED_COLUMNS = Set.of(ALIAS, PUBLISH);
    private static final long PUBLISH_POLL_INTERVAL_MS = 5000L;
    private static final DateTimeFormatter REPORT_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");

    @Option(names = { "-i", "--input-file" }, description = "Input CSV file")
    private Path inputFile;

    @Option(names = { "--alias" }, description = "Alias of the collection to create the dataset in (default: " + DEFAULT_ALIAS + ")")
    private String alias;

    @Option(names = { "--publish" }, description = "Publish the dataset after creation (as version 1.0): yes or no/leave-draft (default: no)")
    private String publish;

    @Option(names = { "--timeout" }, defaultValue = "10s", converter = DatasetEditMetadata.TimeoutConverter.class, description = "Timeout for publication, e.g. 30s, 2m, 1h; without a unit seconds are assumed (default: ${DEFAULT-VALUE})")
    private Duration timeout;

    @Option(names = { "--wait-between-rows" }, converter = DatasetEditMetadata.TimeoutConverter.class, description = "Time to wait after each processed row, except the last, e.g. 30s, 2m; without a unit seconds are assumed; only allowed together with --input-file (default: no wait)")
    private Duration waitBetweenRows;

    @Option(names = { "--reports-dir" }, description = "Directory for batch reports")
    private Path reportsDir;

    @Parameters(arity = "0..*", paramLabel = "FIELD=VALUE", description = "Metadata field assignments", defaultValue = "")
    private List<String> fieldAssignments;

    private final DataverseClient dataverseClient;
    private final CollectionMetadataBlocksProvider metadataBlocksProvider;
    private final Path defaultReportsDir;

    public DataverseCreateDataset(DataverseClient dataverseClient, Path defaultReportsDir) {
        this(dataverseClient, new DataverseCollectionMetadataBlocksProvider(dataverseClient), defaultReportsDir);
    }

    DataverseCreateDataset(DataverseClient dataverseClient, CollectionMetadataBlocksProvider metadataBlocksProvider, Path defaultReportsDir) {
        this.dataverseClient = dataverseClient;
        this.metadataBlocksProvider = metadataBlocksProvider;
        this.defaultReportsDir = defaultReportsDir;
    }

    @Override
    public Integer call() {
        try {
            if (timeout.getQuantity() <= 0) {
                System.err.println("--timeout must be greater than zero");
                return 1;
            }
            if (waitBetweenRows != null && inputFile == null) {
                System.err.println("--wait-between-rows can only be used together with --input-file");
                return 1;
            }
            if (waitBetweenRows != null && waitBetweenRows.getQuantity() < 0) {
                System.err.println("--wait-between-rows must not be negative");
                return 1;
            }

            var defaultFieldValues = parseAssignments(fieldAssignments);
            if (defaultFieldValues.keySet().stream().anyMatch(name -> name.startsWith(EXPECTED_VALUE_PREFIX))) {
                System.err.println("Expected values (=<field>) cannot be used when creating a dataset");
                return 1;
            }
            if (inputFile != null && !defaultFieldValues.isEmpty()) {
                System.err.println("Field values can only be provided in the input file when --input-file is used");
                return 1;
            }
            if (inputFile == null && defaultFieldValues.isEmpty()) {
                System.err.println("Either --input-file or at least one metadata field assignment is required");
                return 1;
            }
            PublishChoice.parse(trimToNull(publish));

            log.info("Starting dataverse-create-dataset in {} mode", inputFile != null ? "batch" : "single-row");
            try (var writer = createReportWriter();
                var batchProcessor = inputFile != null
                    ? BatchProcessor.forCsv(inputFile, writer, SPECIAL_COLUMN_PREFIX)
                    : BatchProcessor.forSingleRow(new LinkedHashMap<>(defaultFieldValues), writer, SPECIAL_COLUMN_PREFIX)) {
                var summary = batchProcessor.process(this::processRow, waitBetweenRows == null ? 0 : waitBetweenRows.toMilliseconds());
                log.info("Finished: {} ok, {} failed, {} skipped", summary.getOkCount(), summary.getFailedCount(), summary.getSkippedCount());
                return summary.getFailedCount() == 0 ? 0 : 1;
            }
        }
        catch (Exception e) {
            log.error("Error creating dataset", e);
            System.err.println("Error creating dataset: " + e.getMessage());
            return 1;
        }
    }

    private Writer createReportWriter() throws IOException {
        if (inputFile == null) {
            return new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
        }
        var effectiveReportsDir = reportsDir != null ? reportsDir : defaultReportsDir;
        if (effectiveReportsDir == null) {
            log.info("No reports directory configured; writing batch report to stdout");
            return new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
        }
        Files.createDirectories(effectiveReportsDir);
        var fileName = inputFile.getFileName().toString();
        var dotIndex = fileName.lastIndexOf('.');
        var basename = dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
        var reportFile = effectiveReportsDir.resolve(basename + "-" + LocalDateTime.now().format(REPORT_TIMESTAMP_FORMAT) + ".csv");
        log.info("Writing batch report to {}", reportFile);
        return Files.newBufferedWriter(reportFile, StandardCharsets.UTF_8);
    }

    private BatchProcessor.Result processRow(BatchProcessor.Row row) throws Exception {
        var specialValues = new HashMap<String, String>();
        var fieldValues = new LinkedHashMap<String, String>();
        var names = row.asMap().keySet();
        for (var entry : row.asMap().entrySet()) {
            // In single-row mode only starred names are special; unstarred ones are field assignments from the command line
            var column = inputFile != null ? toSpecialColumn(entry.getKey(), names, RESERVED_COLUMNS) : null;
            if (column != null && RESERVED_COLUMNS.contains(column)) {
                specialValues.put(column, entry.getValue());
            }
            else if (entry.getKey().startsWith(EXPECTED_VALUE_PREFIX)) {
                throw new IllegalArgumentException("Expected values (" + entry.getKey() + ") cannot be used when creating a dataset");
            }
            else if (entry.getKey().startsWith(SPECIAL_COLUMN_PREFIX)) {
                throw new IllegalArgumentException("Unknown special column: " + entry.getKey());
            }
            else {
                fieldValues.put(entry.getKey(), entry.getValue());
            }
        }

        var collectionAlias = firstNonBlank(specialValues.get(ALIAS), alias, DEFAULT_ALIAS);
        var publishChoice = PublishChoice.parse(firstNonBlank(specialValues.get(PUBLISH), publish, null));

        var blocks = metadataBlocksProvider.getMetadataBlocks(collectionAlias);
        var fieldSpecs = new LinkedHashMap<String, DatasetFieldType>();
        var blockNameByField = new HashMap<String, String>();
        for (var block : blocks) {
            if (block.getFields() != null) {
                fieldSpecs.putAll(block.getFields());
                block.getFields().values().forEach(spec -> blockNameByField.put(spec.getName(), block.getName()));
            }
        }

        var fieldList = toFieldList(fieldValues, fieldSpecs, false, null);
        if (fieldList.getFields().isEmpty()) {
            return BatchProcessor.Result.failed("No metadata fields specified");
        }

        var dataset = toDataset(fieldList, blocks, blockNameByField);
        log.info("Creating dataset in collection {}", collectionAlias);
        var created = dataverseClient.dataverse(collectionAlias).createDataset(dataset).getData();
        var pid = created.getPersistentId();
        log.info("Created dataset {}", pid);

        if (publishChoice == PublishChoice.YES) {
            log.info("Publishing dataset {}", pid);
            var datasetApi = dataverseClient.dataset(pid);
            datasetApi.publish(UpdateType.major, false);
            datasetApi.awaitState("RELEASED", timeout.toMilliseconds(), PUBLISH_POLL_INTERVAL_MS);
            return BatchProcessor.Result.ok("Created and published " + pid);
        }
        return BatchProcessor.Result.ok("Created " + pid);
    }

    private static Dataset toDataset(FieldList fieldList, List<MetadataBlockDefinition> blocks, Map<String, String> blockNameByField) {
        var definitionsByName = new HashMap<String, MetadataBlockDefinition>();
        blocks.forEach(block -> definitionsByName.put(block.getName(), block));
        var metadataBlocks = new LinkedHashMap<String, MetadataBlock>();
        for (MetadataField field : fieldList.getFields()) {
            var blockName = blockNameByField.get(field.getTypeName());
            var block = metadataBlocks.computeIfAbsent(blockName, name -> {
                var newBlock = new MetadataBlock();
                newBlock.setName(name);
                newBlock.setDisplayName(definitionsByName.get(name).getDisplayName());
                newBlock.setFields(new ArrayList<>());
                return newBlock;
            });
            block.getFields().add(field);
        }
        var version = new DatasetVersion();
        version.setMetadataBlocks(metadataBlocks);
        // Dataverse fails with a ClassCastException on "files": null, so it must be an (empty) list
        version.setFiles(new ArrayList<>());
        var dataset = new Dataset();
        dataset.setDatasetVersion(version);
        return dataset;
    }

    private static String firstNonBlank(String first, String second, String third) {
        if (!isBlank(first)) {
            return first.trim();
        }
        if (!isBlank(second)) {
            return second.trim();
        }
        return third;
    }

    interface CollectionMetadataBlocksProvider {
        List<MetadataBlockDefinition> getMetadataBlocks(String alias) throws Exception;
    }

    static class DataverseCollectionMetadataBlocksProvider implements CollectionMetadataBlocksProvider {
        private final DataverseClient dataverseClient;
        private final Map<String, List<MetadataBlockDefinition>> cache = new HashMap<>();

        DataverseCollectionMetadataBlocksProvider(DataverseClient dataverseClient) {
            this.dataverseClient = dataverseClient;
        }

        @Override
        public List<MetadataBlockDefinition> getMetadataBlocks(String alias) throws Exception {
            var cached = cache.get(alias);
            if (cached != null) {
                return cached;
            }
            log.info("Loading metadata field definitions for collection {}", alias);
            var blocks = dataverseClient.dataverse(alias).listMetadataBlocks(false, true).getData();
            cache.put(alias, blocks);
            return blocks;
        }
    }

    enum PublishChoice {
        YES,
        NO;

        static PublishChoice parse(String value) {
            if (value == null) {
                return NO;
            }
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "yes", "true" -> YES;
                case "no", "false", "leave-draft" -> NO;
                default -> throw new IllegalArgumentException("Invalid publish value (expected yes or no/leave-draft): " + value);
            };
        }
    }
}
