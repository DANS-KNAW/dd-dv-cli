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
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

@Command(
    name = "dataset-edit-metadata",
    description = "Edit metadata for a dataset or batch of datasets",
    mixinStandardHelpOptions = true
)
@Slf4j
public class DatasetEditMetadata implements Callable<Integer> {
    private static final String DATASET_ID = "datasetId";
    private static final String EXPECTED_STATE = "expectedState";
    private static final String EXPECT_IN_REVIEW = "expectInReview";
    private static final String PUBLISH_VERSION = "publishVersion";
    private static final String REPLACE = "replace";
    private static final String RESULT = "result";
    private static final String MESSAGE = "message";
    private static final Set<String> RESERVED_COLUMNS = Set.of(DATASET_ID, EXPECTED_STATE, EXPECT_IN_REVIEW, PUBLISH_VERSION, REPLACE);
    private static final Set<String> RESERVED_REPORT_COLUMNS = Set.of(RESULT, MESSAGE);
    private static final Pattern FIELD_PATTERN = Pattern.compile("^([^\\.\\[]+)(?:\\[(\\d+)])?(?:\\.(.+))?$");
    private static final long PUBLISH_POLL_INTERVAL_MS = 5000L;
    private static final DateTimeFormatter REPORT_TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss");

    @Option(names = { "-i", "--input-file" }, description = "Input CSV file")
    private Path inputFile;

    @Option(names = { "--datasetId" }, description = "Dataset database id or persistent identifier")
    private String datasetId;

    @Option(names = { "--expectedState" }, description = "Expected latest version state")
    private String expectedState;

    @Option(names = { "--expectInReview" }, description = "Whether an InReview lock is expected: yes, no, either")
    private String expectInReview;

    @Option(names = { "--publishVersion" }, description = "Publish as major, minor or leave-draft")
    private String publishVersion;

    @Option(names = { "--replace" }, description = "Replace existing values: true/false or yes/no")
    private String replace;

    @Option(names = { "--timeout" }, defaultValue = "10", description = "Timeout in minutes for publication")
    private long timeoutInMinutes;

    @Option(names = { "--reports-dir" }, description = "Directory for batch reports")
    private Path reportsDir;

    @Parameters(arity = "0..*", paramLabel = "FIELD=VALUE", description = "Metadata field assignment defaults")
    private List<String> fieldAssignments = new ArrayList<>();

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
            if (inputFile == null && isBlank(datasetId)) {
                System.err.println("Either --input-file or --datasetId is required");
                return 1;
            }

            var defaultFieldValues = parseAssignments(fieldAssignments);
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

                var summary = batchProcessor.process(row -> processRow(row, defaultFieldValues, fieldSpecs));
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
        var effectiveValues = applyDefaults(row, defaultFieldValues);
        var id = trimToNull(effectiveValues.get(DATASET_ID));
        if (id == null) {
            return BatchProcessor.Result.failed("Missing datasetId");
        }

        log.info("Processing metadata edit for dataset {}", id);
        var datasetApi = getDatasetApi(id);
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
        if (editableValues.isEmpty()) {
            return BatchProcessor.Result.failed("No metadata fields specified");
        }

        var fieldList = toFieldList(editableValues, fieldSpecs);
        var replaceExistingValues = parseBoolean(trimToNull(effectiveValues.get(REPLACE)), false, REPLACE);
        log.debug("Editing {} metadata fields for dataset {} with replace={}", fieldList.getFields().size(), id, replaceExistingValues);
        datasetApi.editMetadata(fieldList, replaceExistingValues);

        var publicationMode = PublishVersion.parse(trimToNull(effectiveValues.get(PUBLISH_VERSION)));
        if (publicationMode != PublishVersion.LEAVE_DRAFT) {
            log.info("Publishing dataset {} as {}", id, publicationMode);
            datasetApi.publish(publicationMode.toUpdateType(), false);
            datasetApi.awaitState("RELEASED", Duration.ofMinutes(timeoutInMinutes).toMillis(), PUBLISH_POLL_INTERVAL_MS);
        }

        log.info("Finished metadata edit for dataset {}", id);
        return BatchProcessor.Result.ok("Metadata edited");
    }

    private LinkedHashMap<String, String> createSingleRow(Map<String, String> defaultFieldValues) {
        var row = new LinkedHashMap<String, String>();
        if (!isBlank(datasetId)) {
            row.put(DATASET_ID, datasetId);
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
        if (replace != null) {
            row.put(REPLACE, replace);
        }
        row.putAll(defaultFieldValues);
        return row;
    }

    private Map<String, String> applyDefaults(BatchProcessor.Row row, Map<String, String> defaultFieldValues) {
        var effectiveValues = new LinkedHashMap<>(row.asMap());
        applyDefault(effectiveValues, row, DATASET_ID, datasetId);
        applyDefault(effectiveValues, row, EXPECTED_STATE, expectedState);
        applyDefault(effectiveValues, row, EXPECT_IN_REVIEW, expectInReview);
        applyDefault(effectiveValues, row, PUBLISH_VERSION, publishVersion);
        applyDefault(effectiveValues, row, REPLACE, replace);

        for (var entry : defaultFieldValues.entrySet()) {
            if (!row.hasColumn(entry.getKey())) {
                effectiveValues.put(entry.getKey(), entry.getValue());
            }
        }
        return effectiveValues;
    }

    private void applyDefault(Map<String, String> effectiveValues, BatchProcessor.Row row, String column, String value) {
        if (!row.hasColumn(column) && value != null) {
            effectiveValues.put(column, value);
        }
    }

    private Map<String, String> parseAssignments(List<String> assignments) {
        var parsed = new LinkedHashMap<String, String>();
        for (String assignment : assignments) {
            int separator = assignment.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("Invalid field assignment: " + assignment);
            }
            String fieldName = assignment.substring(0, separator);
            if (RESERVED_COLUMNS.contains(fieldName) || RESERVED_REPORT_COLUMNS.contains(fieldName)) {
                throw new IllegalArgumentException("Reserved field name: " + fieldName);
            }
            parsed.put(fieldName, assignment.substring(separator + 1));
        }
        return parsed;
    }

    private Map<String, String> extractEditableValues(Map<String, String> effectiveValues) {
        var values = new LinkedHashMap<String, String>();
        for (var entry : effectiveValues.entrySet()) {
            if (!RESERVED_COLUMNS.contains(entry.getKey()) && trimToNull(entry.getValue()) != null) {
                values.put(entry.getKey(), entry.getValue());
            }
        }
        return values;
    }

    private FieldList toFieldList(Map<String, String> editableValues, Map<String, DatasetFieldType> fieldSpecs) {
        var accumulators = new LinkedHashMap<String, FieldValueAccumulator>();

        for (var entry : editableValues.entrySet()) {
            var matcher = FIELD_PATTERN.matcher(entry.getKey());
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid field column: " + entry.getKey());
            }

            var fieldName = matcher.group(1);
            var indexText = matcher.group(2);
            var subfieldName = matcher.group(3);
            var spec = fieldSpecs.get(fieldName);
            if (spec == null) {
                throw new IllegalArgumentException("Unknown metadata field: " + fieldName);
            }

            int index = determineIndex(spec, indexText, entry.getKey());
            var accumulator = accumulators.computeIfAbsent(fieldName, ignored -> new FieldValueAccumulator(spec));
            var value = trimToNull(entry.getValue());
            if (value == null) {
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
            fieldList.add(accumulator.toMetadataField());
        }
        return fieldList;
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

            var subfieldSpec = fieldSpec.getChildFields() != null ? fieldSpec.getChildFields().get(subfieldName) : null;
            if (subfieldSpec == null) {
                throw new IllegalArgumentException("Unknown subfield " + subfieldName + " for " + fieldSpec.getName());
            }
            if ("compound".equals(subfieldSpec.getTypeClass()) || subfieldSpec.isMultiple()) {
                throw new IllegalArgumentException("Unsupported subfield shape for " + columnName);
            }

            var values = compoundValues.computeIfAbsent(index, ignored -> new LinkedHashMap<>());
            if (values.putIfAbsent(subfieldName, value) != null) {
                throw new IllegalArgumentException("Duplicate field value for " + columnName);
            }
        }

        MetadataField toMetadataField() {
            if ("compound".equals(fieldSpec.getTypeClass())) {
                return toCompoundField();
            }
            return toSimpleField();
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
                case "leave-draft" -> LEAVE_DRAFT;
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
                return NO;
            }

            return switch (value.toLowerCase(Locale.ROOT)) {
                case "yes", "true" -> YES;
                case "no", "false" -> NO;
                case "either" -> EITHER;
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
                return new ExpectedState("RELEASED", false);
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
            boolean matches = state.equalsIgnoreCase(actualState);
            return negated != matches;
        }

        String describe() {
            return negated ? "not " + state : state;
        }
    }
}
