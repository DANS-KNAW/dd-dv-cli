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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DatasetApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.MetadataBlocksApi;
import nl.knaw.dans.lib.dataverse.Version;
import nl.knaw.dans.lib.dataverse.model.Lock;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetFieldType;
import nl.knaw.dans.lib.dataverse.model.dataset.FieldList;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.SingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.UpdateType;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Command(
    name = "dataset-edit-metadata",
    description = "Edit metadata for a dataset or batch of datasets",
    mixinStandardHelpOptions = true
)
@Slf4j
public class DatasetEditMetadata implements Callable<Integer> {
    private static final String DATASET_ID = "datasetId";
    private static final String PID = "pid";
    private static final String DATABASE_ID = "databaseId";
    private static final List<String> ID_COLUMNS = List.of(DATASET_ID, PID, DATABASE_ID);
    private static final Pattern PID_PATTERN = Pattern.compile("^[A-Za-z][A-Za-z0-9]*:\\S.*$");
    private static final Pattern DATABASE_ID_PATTERN = Pattern.compile("^\\d+$");
    private static final String EXPECTED_STATE = "expectedState";
    private static final String EXPECT_IN_REVIEW = "expectInReview";
    private static final String PUBLISH_VERSION = "publishVersion";
    private static final String VERSION_NOTE = "versionNote";
    private static final String REPLACE = "replace";
    private static final String RESULT = "result";
    private static final String MESSAGE = "message";
    private static final Set<String> RESERVED_COLUMNS = Set.of(DATASET_ID, PID, DATABASE_ID, EXPECTED_STATE, EXPECT_IN_REVIEW, PUBLISH_VERSION, VERSION_NOTE, REPLACE);
    private static final Set<String> RESERVED_REPORT_COLUMNS = Set.of(RESULT, MESSAGE);
    private static final Pattern FIELD_PATTERN = Pattern.compile("^(?<field>[^.\\[]+)(?:\\[(?<index>\\d+)])?(?:\\.(?<subfield>.+))?$");
    private static final long PUBLISH_POLL_INTERVAL_MS = 5000L;
    private static final DateTimeFormatter REPORT_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");

    @Option(names = { "-i", "--input-file" }, description = "Input CSV file")
    private Path inputFile;

    @Option(names = { "--datasetId" }, description = "Dataset database id or persistent identifier")
    private String datasetId;

    @Option(names = { "--pid" }, description = "Dataset persistent identifier, including the protocol, e.g., doi:10.5072/FK2/ABC")
    private String pid;

    @Option(names = { "--databaseId" }, description = "Dataset database id (digits only)")
    private String databaseId;

    @Option(names = { "--expectedState" }, description = "Expected latest version state: (not) draft, released, deaccessioned; omit for no expectation")
    private String expectedState;

    @Option(names = { "--expectInReview" }, description = "Whether an InReview lock is expected: yes, no; omit for no expectation")
    private String expectInReview;

    @Option(names = { "--publishVersion" }, description = "Publish as major, minor or leave-draft (no is a synonym for leave-draft)")
    private String publishVersion;

    @Option(names = { "--versionNote" }, description = "Version note to set on the draft before publication")
    private String versionNote;

    @Option(names = { "--replace" }, description = "Replace existing values")
    private boolean replace;

    @Option(names = { "--skip-code" }, defaultValue = "!skip", description = "Cell value that marks a field as not to be changed (default: ${DEFAULT-VALUE})")
    private String skipCode;

    @Option(names = { "--timeout" }, defaultValue = "10s", converter = TimeoutConverter.class, description = "Timeout for publication, e.g. 30s, 2m, 1h; without a unit seconds are assumed (default: ${DEFAULT-VALUE})")
    private Duration timeout;

    @Option(names = { "--wait-between-rows" }, converter = TimeoutConverter.class, description = "Time to wait after each processed row, except the last, e.g. 30s, 2m; without a unit seconds are assumed; only allowed together with --input-file (default: no wait)")
    private Duration waitBetweenRows;

    @Option(names = { "--reports-dir" }, description = "Directory for batch reports")
    private Path reportsDir;

    @Parameters(arity = "0..*", paramLabel = "FIELD=VALUE", description = "Metadata field assignment defaults", defaultValue = "")
    private List<String> fieldAssignments;

    private final DataverseClient dataverseClient;
    private final MetadataFieldSpecProvider metadataFieldSpecProvider;
    private final Path defaultReportsDir;

    public DatasetEditMetadata(DataverseClient dataverseClient, Path defaultReportsDir) {
        this(dataverseClient, new DataverseMetadataFieldSpecProvider(dataverseClient.metadataBlocks()), defaultReportsDir);
    }

    DatasetEditMetadata(DataverseClient dataverseClient, MetadataFieldSpecProvider metadataFieldSpecProvider, Path defaultReportsDir) {
        this.dataverseClient = dataverseClient;
        this.metadataFieldSpecProvider = metadataFieldSpecProvider;
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

            var idOptionCount = Stream.of(datasetId, pid, databaseId).filter(value -> !isBlank(value)).count();
            if (idOptionCount > 1) {
                System.err.println("--datasetId, --pid and --databaseId are mutually exclusive");
                return 1;
            }
            if (inputFile != null && idOptionCount > 0) {
                System.err.println("--datasetId, --pid and --databaseId cannot be used together with --input-file; put the identifiers in the input file");
                return 1;
            }
            if (inputFile == null && idOptionCount == 0) {
                System.err.println("Either --input-file or one of --datasetId, --pid, --databaseId is required");
                return 1;
            }

            var defaultFieldValues = parseAssignments(fieldAssignments);
            if (inputFile != null && !defaultFieldValues.isEmpty()) {
                System.err.println("Field values can only be provided in the input file when --input-file is used");
                return 1;
            }
            log.info("Starting dataset-edit-metadata in {} mode", inputFile != null ? "batch" : "single-row");
            if (inputFile == null && defaultFieldValues.isEmpty()) {
                System.err.println("At least one metadata field assignment is required");
                return 1;
            }

            var fieldSpecs = metadataFieldSpecProvider.getFieldSpecs();
            try (var writer = createReportWriter();
                var batchProcessor = inputFile != null
                    ? BatchProcessor.forCsv(inputFile, writer)
                    : BatchProcessor.forSingleRow(createSingleRow(defaultFieldValues), writer)) {

                if (inputFile != null) {
                    var idColumnCount = ID_COLUMNS.stream().filter(column -> containsIgnoreCase(Set.copyOf(batchProcessor.getHeaders()), column)).count();
                    if (idColumnCount > 1) {
                        System.err.println("Input file must contain only one of the columns datasetId, pid and databaseId");
                        return 1;
                    }
                    if (idColumnCount == 0) {
                        System.err.println("Input file must contain one of the columns datasetId, pid and databaseId");
                        return 1;
                    }
                }

                var summary = batchProcessor.process(row -> processRow(row, defaultFieldValues, fieldSpecs), waitBetweenRows == null ? 0 : waitBetweenRows.toMilliseconds());
                log.info("Finished: {} ok, {} failed, {} skipped", summary.getOkCount(), summary.getFailedCount(), summary.getSkippedCount());
                return summary.getFailedCount() == 0 ? 0 : 1;
            }
        }
        catch (Exception e) {
            log.error("Error editing dataset metadata", e);
            System.err.println("Error editing dataset metadata: " + e.getMessage());
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
        var reportFile = effectiveReportsDir.resolve(createReportFileName(inputFile));
        log.info("Writing batch report to {}", reportFile);
        return Files.newBufferedWriter(reportFile, StandardCharsets.UTF_8);
    }

    private String createReportFileName(Path csvInputFile) {
        var fileName = csvInputFile.getFileName().toString();
        var dotIndex = fileName.lastIndexOf('.');
        var basename = dotIndex > 0 ? fileName.substring(0, dotIndex) : fileName;
        return basename + "-" + LocalDateTime.now().format(REPORT_TIMESTAMP_FORMAT) + ".csv";
    }

    private BatchProcessor.Result processRow(BatchProcessor.Row row, Map<String, String> defaultFieldValues, Map<String, DatasetFieldType> fieldSpecs) throws Exception {
        var effectiveValues = applyDefaults(row);
        var idColumns = ID_COLUMNS.stream().filter(column -> trimToNull(effectiveValues.get(column)) != null).toList();
        if (idColumns.isEmpty()) {
            return BatchProcessor.Result.failed("Missing dataset identifier: one of datasetId, pid or databaseId is required");
        }
        if (idColumns.size() > 1) {
            return BatchProcessor.Result.failed("Only one of datasetId, pid and databaseId may be specified");
        }
        var idColumn = idColumns.get(0);
        var id = trimToNull(effectiveValues.get(idColumn));
        if (PID.equals(idColumn) && !PID_PATTERN.matcher(id).matches()) {
            return BatchProcessor.Result.failed("Invalid pid, it must start with a protocol label such as 'doi:': " + id);
        }
        if (DATABASE_ID.equals(idColumn) && !DATABASE_ID_PATTERN.matcher(id).matches()) {
            return BatchProcessor.Result.failed("Invalid databaseId, it must consist of digits only: " + id);
        }

        log.info("Processing metadata edit for dataset {}", id);
        var datasetApi = switch (idColumn) {
            case PID -> dataverseClient.dataset(id);
            case DATABASE_ID -> dataverseClient.dataset(Integer.parseInt(id));
            default -> getDatasetApi(id);
        };
        var latestVersion = datasetApi.getVersion(Version.LATEST.toString()).getData();

        var actualState = latestVersion.getVersionState();
        var expectedStateMatcher = ExpectedState.parse(trimToNull(effectiveValues.get(EXPECTED_STATE)));
        if (!expectedStateMatcher.matches(actualState)) {
            log.info("Skipping dataset {} because state {} does not match expected {}", id, actualState, expectedStateMatcher.describe());
            return BatchProcessor.Result.skipped(String.format("Expected state %s but found %s", expectedStateMatcher.describe(), actualState));
        }

        var reviewExpectation = ReviewExpectation.parse(trimToNull(effectiveValues.get(EXPECT_IN_REVIEW)));
        var inReview = isInReview(datasetApi.getLocks().getData());
        if (!reviewExpectation.matches(inReview)) {
            log.info("Skipping dataset {} because in-review={} does not match expected {}", id, inReview, reviewExpectation.describe());
            return BatchProcessor.Result.skipped(String.format("Expected in review %s but found %s", reviewExpectation.describe(), inReview ? "yes" : "no"));
        }

        var editableValues = extractEditableValues(effectiveValues);
        var replaceExistingValues = parseBoolean(trimToNull(effectiveValues.get(REPLACE)), false, REPLACE);
        var fieldList = toFieldList(editableValues, fieldSpecs, replaceExistingValues);
        if (fieldList.getFields().isEmpty()) {
            return BatchProcessor.Result.failed("No metadata fields specified");
        }
        log.debug("Editing {} metadata fields for dataset {} with replace={}", fieldList.getFields().size(), id, replaceExistingValues);
        datasetApi.editMetadata(fieldList, replaceExistingValues);

        var note = trimToNull(effectiveValues.get(VERSION_NOTE));
        if (note != null) {
            log.debug("Setting version note for dataset {}", id);
            datasetApi.setVersionNote(note);
        }

        var publicationMode = PublishVersion.parse(trimToNull(effectiveValues.get(PUBLISH_VERSION)));
        if (publicationMode != PublishVersion.LEAVE_DRAFT) {
            log.info("Publishing dataset {} as {}", id, publicationMode);
            datasetApi.publish(publicationMode.toUpdateType(), false);
            datasetApi.awaitState("RELEASED", timeout.toMilliseconds(), PUBLISH_POLL_INTERVAL_MS);
        }

        log.info("Finished metadata edit for dataset {}", id);
        return BatchProcessor.Result.ok("Metadata edited");
    }

    private LinkedHashMap<String, String> createSingleRow(Map<String, String> defaultFieldValues) {
        var row = new LinkedHashMap<String, String>();
        if (!isBlank(datasetId)) {
            row.put(DATASET_ID, datasetId);
        }
        if (!isBlank(pid)) {
            row.put(PID, pid);
        }
        if (!isBlank(databaseId)) {
            row.put(DATABASE_ID, databaseId);
        }
        if (expectedState != null) {
            row.put(EXPECTED_STATE, expectedState);
        }
        if (expectInReview != null) {
            row.put(EXPECT_IN_REVIEW, expectInReview);
        }
        if (publishVersion != null) {
            row.put(PUBLISH_VERSION, publishVersion);
        }
        if (versionNote != null) {
            row.put(VERSION_NOTE, versionNote);
        }
        row.put(REPLACE, Boolean.toString(replace));
        row.putAll(defaultFieldValues);
        return row;
    }

    private Map<String, String> applyDefaults(BatchProcessor.Row row) {
        var effectiveValues = new LinkedHashMap<String, String>();
        row.asMap().forEach((name, value) -> {
            var column = RESERVED_COLUMNS.stream().filter(name::equalsIgnoreCase).findFirst().orElse(name);
            effectiveValues.put(column, value);
        });
        if (inputFile == null) {
            applyDefault(effectiveValues, row, DATASET_ID, datasetId);
            applyDefault(effectiveValues, row, PID, pid);
            applyDefault(effectiveValues, row, DATABASE_ID, databaseId);
        }
        applyDefault(effectiveValues, row, EXPECTED_STATE, expectedState);
        applyDefault(effectiveValues, row, EXPECT_IN_REVIEW, expectInReview);
        applyDefault(effectiveValues, row, PUBLISH_VERSION, publishVersion);
        applyDefault(effectiveValues, row, VERSION_NOTE, versionNote);
        applyDefault(effectiveValues, row, REPLACE, Boolean.toString(replace));
        return effectiveValues;
    }

    private void applyDefault(Map<String, String> effectiveValues, BatchProcessor.Row row, String column, String value) {
        if (value != null && (!row.hasColumn(column) || isBlank(row.getValue(column)))) {
            effectiveValues.put(column, value);
        }
    }

    private Map<String, String> parseAssignments(List<String> assignments) {
        var parsed = new LinkedHashMap<String, String>();
        for (String assignment : assignments) {
            if (assignment.isBlank()) {
                continue;
            }

            int separator = assignment.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("Invalid field assignment: " + assignment);
            }
            String fieldName = assignment.substring(0, separator);
            if (containsIgnoreCase(RESERVED_COLUMNS, fieldName) || containsIgnoreCase(RESERVED_REPORT_COLUMNS, fieldName)) {
                throw new IllegalArgumentException("Reserved field name: " + fieldName);
            }
            parsed.put(fieldName, assignment.substring(separator + 1));
        }
        return parsed;
    }

    private Map<String, String> extractEditableValues(Map<String, String> effectiveValues) {
        var values = new LinkedHashMap<String, String>();
        for (var entry : effectiveValues.entrySet()) {
            if (!containsIgnoreCase(RESERVED_COLUMNS, entry.getKey())) {
                values.put(entry.getKey(), entry.getValue());
            }
        }
        return values;
    }

    private FieldList toFieldList(Map<String, String> editableValues, Map<String, DatasetFieldType> fieldSpecs, boolean replaceExistingValues) {
        var accumulators = new LinkedHashMap<String, FieldValueAccumulator>();

        for (var entry : editableValues.entrySet()) {
            var value = trimToNull(entry.getValue());
            if (value != null && value.equals(skipCode)) {
                markSkipped(entry.getKey(), fieldSpecs, accumulators);
                continue;
            }
            if (value == null && !replaceExistingValues) {
                continue;
            }

            var matcher = FIELD_PATTERN.matcher(entry.getKey());
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid field column: " + entry.getKey());
            }

            var fieldName = matcher.group("field");
            var indexText = matcher.group("index");
            var subfieldName = matcher.group("subfield");
            var spec = getFieldSpecIgnoreCase(fieldSpecs, fieldName);
            if (spec == null) {
                throw new IllegalArgumentException("Unknown metadata field: " + fieldName);
            }

            int index = determineIndex(spec, indexText, entry.getKey());
            var accumulator = accumulators.computeIfAbsent(spec.getName(), ignored -> new FieldValueAccumulator(spec));
            if (value == null) {
                if (subfieldName != null) {
                    if (!"compound".equals(spec.getTypeClass())) {
                        throw new IllegalArgumentException("Subfield not allowed for " + entry.getKey());
                    }
                    var subfieldSpec = getFieldSpecIgnoreCase(spec.getChildFields(), subfieldName);
                    if (subfieldSpec == null) {
                        throw new IllegalArgumentException("Unknown subfield " + subfieldName + " for " + spec.getName());
                    }
                    if ("compound".equals(subfieldSpec.getTypeClass()) || subfieldSpec.isMultiple()) {
                        throw new IllegalArgumentException("Unsupported subfield shape for " + entry.getKey());
                    }
                }
                accumulator.markBlank();
                continue;
            }

            if (subfieldName == null) {
                accumulator.addValue(index, value, entry.getKey());
            }
            else {
                accumulator.addSubfieldValue(index, subfieldName, value, entry.getKey());
            }
        }

        var fieldList = new FieldList();
        for (FieldValueAccumulator accumulator : accumulators.values()) {
            if (accumulator.isChanged()) {
                fieldList.add(accumulator.toMetadataField());
            }
        }
        return fieldList;
    }

    private void markSkipped(String columnName, Map<String, DatasetFieldType> fieldSpecs, Map<String, FieldValueAccumulator> accumulators) {
        var spec = getFieldSpecIgnoreCase(fieldSpecs, rootFieldName(columnName));
        if (spec == null) {
            throw new IllegalArgumentException("Unknown metadata field: " + rootFieldName(columnName));
        }
        accumulators.computeIfAbsent(spec.getName(), ignored -> new FieldValueAccumulator(spec)).markSkipped();
    }

    private static String rootFieldName(String columnName) {
        var matcher = FIELD_PATTERN.matcher(columnName);
        return matcher.matches() ? matcher.group("field") : columnName;
    }

    private int determineIndex(DatasetFieldType spec, String indexText, String columnName) {
        if (indexText == null) {
            return 1;
        }

        if (!spec.isMultiple()) {
            throw new IllegalArgumentException("Field does not allow indexed values: " + columnName);
        }
        int index = Integer.parseInt(indexText);
        if (index < 1) {
            throw new IllegalArgumentException("Field indexes must start at 1: " + columnName);
        }
        return index;
    }

    private boolean isInReview(List<Lock> locks) {
        if (locks == null) {
            return false;
        }
        return locks.stream()
            .map(Lock::getLockType)
            .filter(Objects::nonNull)
            .anyMatch("InReview"::equalsIgnoreCase);
    }

    @SuppressWarnings("SameParameterValue")
    private boolean parseBoolean(String value, boolean defaultValue, String optionName) {
        if (value == null) {
            return defaultValue;
        }

        return switch (value.toLowerCase(Locale.ROOT)) {
            case "true", "yes" -> true;
            case "false", "no" -> false;
            default -> throw new IllegalArgumentException("Invalid value for " + optionName + ": " + value);
        };
    }

    private DatasetApi getDatasetApi(String value) {
        try {
            return dataverseClient.dataset(Integer.parseInt(value));
        }
        catch (NumberFormatException e) {
            return dataverseClient.dataset(value);
        }
    }

    private static boolean containsIgnoreCase(Set<String> names, String name) {
        return names.stream().anyMatch(name::equalsIgnoreCase);
    }

    private static DatasetFieldType getFieldSpecIgnoreCase(Map<String, DatasetFieldType> specs, String name) {
        if (specs == null) {
            return null;
        }
        return specs.entrySet().stream()
            .filter(entry -> entry.getKey().equalsIgnoreCase(name))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        var trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static boolean isBlank(String value) {
        return trimToNull(value) == null;
    }

    interface MetadataFieldSpecProvider {
        Map<String, DatasetFieldType> getFieldSpecs() throws Exception;
    }

    @RequiredArgsConstructor
    static class DataverseMetadataFieldSpecProvider implements MetadataFieldSpecProvider {
        private final MetadataBlocksApi metadataBlocksApi;
        private Map<String, DatasetFieldType> cachedFieldSpecs;

        @Override
        public Map<String, DatasetFieldType> getFieldSpecs() throws Exception {
            if (cachedFieldSpecs != null) {
                log.debug("Using cached metadata field definitions");
                return cachedFieldSpecs;
            }

            log.info("Loading metadata field definitions");
            var response = metadataBlocksApi.listMetadataBlocks(false, true);
            var specs = new LinkedHashMap<String, DatasetFieldType>();
            for (var block : response.getData()) {
                if (block.getFields() != null) {
                    specs.putAll(block.getFields());
                }
            }
            cachedFieldSpecs = Collections.unmodifiableMap(specs);
            log.info("Loaded {} metadata field definitions", cachedFieldSpecs.size());
            return cachedFieldSpecs;
        }
    }

    @RequiredArgsConstructor
    static class FieldValueAccumulator {
        private final DatasetFieldType fieldSpec;
        private final Map<Integer, String> simpleValues = new TreeMap<>();
        private final Map<Integer, Map<String, String>> compoundValues = new TreeMap<>();
        private boolean blank;
        private boolean skipped;

        void addValue(int index, String value, String columnName) {
            if ("compound".equals(fieldSpec.getTypeClass())) {
                throw new IllegalArgumentException("Compound field requires subfields: " + columnName);
            }
            if (simpleValues.putIfAbsent(index, value) != null) {
                throw new IllegalArgumentException("Duplicate field value for " + columnName);
            }
        }

        void addSubfieldValue(int index, String subfieldName, String value, String columnName) {
            if (!"compound".equals(fieldSpec.getTypeClass())) {
                throw new IllegalArgumentException("Subfield not allowed for " + columnName);
            }

            var subfieldSpec = getFieldSpecIgnoreCase(fieldSpec.getChildFields(), subfieldName);
            if (subfieldSpec == null) {
                throw new IllegalArgumentException("Unknown subfield " + subfieldName + " for " + fieldSpec.getName());
            }
            if ("compound".equals(subfieldSpec.getTypeClass()) || subfieldSpec.isMultiple()) {
                throw new IllegalArgumentException("Unsupported subfield shape for " + columnName);
            }

            var values = compoundValues.computeIfAbsent(index, ignored -> new LinkedHashMap<>());
            if (values.putIfAbsent(subfieldSpec.getName(), value) != null) {
                throw new IllegalArgumentException("Duplicate field value for " + columnName);
            }
        }

        void markBlank() {
            blank = true;
        }

        void markSkipped() {
            skipped = true;
        }

        boolean isChanged() {
            if (!simpleValues.isEmpty() || !compoundValues.isEmpty()) {
                return true;
            }
            return blank && !skipped;
        }

        MetadataField toMetadataField() {
            if (simpleValues.isEmpty() && compoundValues.isEmpty()) {
                return clearField();
            }
            if ("compound".equals(fieldSpec.getTypeClass())) {
                return toCompoundField();
            }
            return toSimpleField();
        }

        private MetadataField clearField() {
            if ("compound".equals(fieldSpec.getTypeClass())) {
                return fieldSpec.isMultiple()
                    ? new CompoundMultiValueField(fieldSpec.getName(), List.of())
                    : new CompoundSingleValueField(fieldSpec.getName(), Map.of());
            }
            if (fieldSpec.isMultiple()) {
                return switch (fieldSpec.getTypeClass()) {
                    case "primitive" -> new PrimitiveMultiValueField(fieldSpec.getName(), List.of());
                    case "controlledVocabulary" -> new ControlledMultiValueField(fieldSpec.getName(), List.of());
                    default -> throw new IllegalArgumentException("Unsupported field typeClass for " + fieldSpec.getName() + ": " + fieldSpec.getTypeClass());
                };
            }
            return switch (fieldSpec.getTypeClass()) {
                case "primitive" -> new PrimitiveSingleValueField(fieldSpec.getName(), "");
                case "controlledVocabulary" -> new ControlledSingleValueField(fieldSpec.getName(), "");
                default -> throw new IllegalArgumentException("Unsupported field typeClass for " + fieldSpec.getName() + ": " + fieldSpec.getTypeClass());
            };
        }

        private MetadataField toSimpleField() {
            if (simpleValues.isEmpty()) {
                throw new IllegalArgumentException("No value provided for " + fieldSpec.getName());
            }

            if (!fieldSpec.isMultiple()) {
                if (simpleValues.size() > 1) {
                    throw new IllegalArgumentException("Field does not allow multiple values: " + fieldSpec.getName());
                }
                return (MetadataField) createSingleValueField(fieldSpec, simpleValues.values().iterator().next());
            }

            var values = new ArrayList<String>();
            for (String value : simpleValues.values()) {
                validateControlledVocabulary(fieldSpec, value);
                values.add(value);
            }

            return switch (fieldSpec.getTypeClass()) {
                case "primitive" -> new PrimitiveMultiValueField(fieldSpec.getName(), values);
                case "controlledVocabulary" -> new ControlledMultiValueField(fieldSpec.getName(), values);
                default -> throw new IllegalArgumentException("Unsupported field typeClass for " + fieldSpec.getName() + ": " + fieldSpec.getTypeClass());
            };
        }

        private MetadataField toCompoundField() {
            if (compoundValues.isEmpty()) {
                throw new IllegalArgumentException("No value provided for " + fieldSpec.getName());
            }

            if (!fieldSpec.isMultiple()) {
                if (compoundValues.size() > 1) {
                    throw new IllegalArgumentException("Field does not allow multiple values: " + fieldSpec.getName());
                }
                return new CompoundSingleValueField(fieldSpec.getName(), createCompoundValue(compoundValues.values().iterator().next()));
            }

            var values = new ArrayList<Map<String, SingleValueField>>();
            for (Map<String, String> compoundValue : compoundValues.values()) {
                values.add(createCompoundValue(compoundValue));
            }
            return new CompoundMultiValueField(fieldSpec.getName(), values);
        }

        private Map<String, SingleValueField> createCompoundValue(Map<String, String> compoundValue) {
            var values = new LinkedHashMap<String, SingleValueField>();
            for (var entry : compoundValue.entrySet()) {
                var subfieldSpec = fieldSpec.getChildFields() != null ? fieldSpec.getChildFields().get(entry.getKey()) : null;
                values.put(entry.getKey(), createSingleValueField(subfieldSpec, entry.getValue()));
            }
            return values;
        }

        private SingleValueField createSingleValueField(DatasetFieldType spec, String value) {
            validateControlledVocabulary(spec, value);

            return switch (spec.getTypeClass()) {
                case "primitive" -> new PrimitiveSingleValueField(spec.getName(), value);
                case "controlledVocabulary" -> new ControlledSingleValueField(spec.getName(), value);
                default -> throw new IllegalArgumentException("Unsupported field typeClass for " + spec.getName() + ": " + spec.getTypeClass());
            };
        }

        private void validateControlledVocabulary(DatasetFieldType spec, String value) {
            if (!"controlledVocabulary".equals(spec.getTypeClass()) || spec.getControlledVocabularyValues() == null || spec.getControlledVocabularyValues().isEmpty()) {
                return;
            }
            if (!spec.getControlledVocabularyValues().contains(value)) {
                throw new IllegalArgumentException("Invalid controlled vocabulary value for " + spec.getName() + ": " + value);
            }
        }
    }

    static class TimeoutConverter implements ITypeConverter<Duration> {
        @Override
        public Duration convert(String value) {
            var trimmed = value.trim();
            return Duration.parse(trimmed.matches("\\d+") ? trimmed + "s" : trimmed);
        }
    }

    enum PublishVersion {
        MINOR,
        MAJOR,
        LEAVE_DRAFT;

        static PublishVersion parse(String value) {
            if (value == null) {
                return LEAVE_DRAFT;
            }

            return switch (value.toLowerCase(Locale.ROOT)) {
                case "minor" -> MINOR;
                case "major" -> MAJOR;
                case "leave-draft", "no" -> LEAVE_DRAFT;
                default -> throw new IllegalArgumentException("Invalid publishVersion: " + value);
            };
        }

        UpdateType toUpdateType() {
            return switch (this) {
                case MINOR -> UpdateType.minor;
                case MAJOR -> UpdateType.major;
                case LEAVE_DRAFT -> throw new IllegalStateException("leave-draft cannot be converted to an update type");
            };
        }
    }

    enum ReviewExpectation {
        YES,
        NO,
        EITHER;

        static ReviewExpectation parse(String value) {
            if (value == null) {
                return EITHER;
            }

            return switch (value.toLowerCase(Locale.ROOT)) {
                case "yes", "true" -> YES;
                case "no", "false" -> NO;
                default -> throw new IllegalArgumentException("Invalid expectInReview: " + value);
            };
        }

        boolean matches(boolean inReview) {
            return switch (this) {
                case YES -> inReview;
                case NO -> !inReview;
                case EITHER -> true;
            };
        }

        String describe() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    static class ExpectedState {
        private final String state;
        private final boolean negated;

        private ExpectedState(String state, boolean negated) {
            this.state = state;
            this.negated = negated;
        }

        static ExpectedState parse(String value) {
            if (value == null) {
                return new ExpectedState("ANY", false);
            }

            var normalized = value.trim();
            boolean negated = normalized.regionMatches(true, 0, "not ", 0, 4);
            var state = (negated ? normalized.substring(4) : normalized).trim().toUpperCase(Locale.ROOT);
            if (!Set.of("DRAFT", "RELEASED", "DEACCESSIONED").contains(state)) {
                throw new IllegalArgumentException("Invalid expectedState: " + value);
            }
            return new ExpectedState(state, negated);
        }

        boolean matches(String actualState) {
            if ("ANY".equals(state)) {
                return true;
            }
                            boolean matches = state.equalsIgnoreCase(actualState);
            return negated != matches;
        }

        String describe() {
            return negated ? "not " + state : state;
        }
    }
}
