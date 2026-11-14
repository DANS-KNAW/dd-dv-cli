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
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.SingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.UpdateType;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static nl.knaw.dans.dvcli.command.DatasetFieldConverter.FIELD_PATTERN;
import static nl.knaw.dans.dvcli.command.DatasetFieldConverter.getFieldSpecIgnoreCase;
import static nl.knaw.dans.dvcli.command.DatasetFieldConverter.toFieldList;
import static nl.knaw.dans.dvcli.command.MetadataColumns.EXPECTED_VALUE_PREFIX;
import static nl.knaw.dans.dvcli.command.MetadataColumns.SPECIAL_COLUMN_PREFIX;
import static nl.knaw.dans.dvcli.command.MetadataColumns.containsIgnoreCase;
import static nl.knaw.dans.dvcli.command.MetadataColumns.parseAssignments;
import static nl.knaw.dans.dvcli.command.Strings.isBlank;
import static nl.knaw.dans.dvcli.command.Strings.trimToNull;

@Command(
    name = "dataset-edit-metadata",
    description = "Edit metadata for a dataset or batch of datasets",
    mixinStandardHelpOptions = true
)
@Slf4j
public class DatasetEditMetadata implements Callable<Integer> {
    private static final String DATASET_ID = "*datasetId";
    private static final String PID = "*pid";
    private static final String DATABASE_ID = "*databaseId";
    private static final List<String> ID_COLUMNS = List.of(DATASET_ID, PID, DATABASE_ID);
    private static final Pattern PID_PATTERN = Pattern.compile("^[A-Za-z][A-Za-z0-9]*:\\S.*$");
    private static final Pattern DATABASE_ID_PATTERN = Pattern.compile("^\\d+$");
    private static final String EXPECTED_STATE = "*expectedState";
    private static final String EXPECT_IN_REVIEW = "*expectInReview";
    private static final String PUBLISH_VERSION = "*publishVersion";
    private static final String VERSION_NOTE = "*versionNote";
    private static final String REPLACE = "*replace";
    private static final Set<String> RESERVED_COLUMNS = Set.of(DATASET_ID, PID, DATABASE_ID, EXPECTED_STATE, EXPECT_IN_REVIEW, PUBLISH_VERSION, VERSION_NOTE, REPLACE);
    private static final long PUBLISH_POLL_INTERVAL_MS = 5000L;

    @Option(names = { "-i", "--input-file" }, description = "Input CSV file")
    private Path inputFile;

    @Option(names = { "--datasetId" }, paramLabel = "<databaseIdOrPid>", description = "Dataset database id or persistent identifier")
    private String datasetId;

    @Option(names = { "--pid" }, description = "Dataset persistent identifier, including the protocol, e.g., doi:10.5072/FK2/ABC")
    private String pid;

    @Option(names = { "--databaseId" }, description = "Dataset database id (digits only)")
    private String databaseId;

    @Option(names = { "--expectedState" }, description = "Expected latest version state: (not) draft, released, deaccessioned; default: not deaccessioned")
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
            log.info("Starting dataset-edit-metadata in {} mode", inputFile != null ? "batch" : "single-row");
            if (inputFile == null && defaultFieldValues.isEmpty()) {
                System.err.println("At least one metadata field assignment is required");
                return 1;
            }

            var fieldSpecs = metadataFieldSpecProvider.getFieldSpecs();
            try (var writer = BatchReportWriters.create(inputFile, reportsDir, defaultReportsDir);
                var batchProcessor = inputFile != null
                    ? BatchProcessor.forCsv(inputFile, writer, SPECIAL_COLUMN_PREFIX, createBatchDefaults(defaultFieldValues))
                    : BatchProcessor.forSingleRow(createSingleRow(defaultFieldValues), writer, SPECIAL_COLUMN_PREFIX)) {

                if (inputFile != null) {
                    var idColumnCount = ID_COLUMNS.stream().filter(column -> batchProcessor.getHeaders().stream().anyMatch(header -> column.equals(toSpecialColumn(header, batchProcessor.getHeaders())))).count();
                    if (idColumnCount > 1) {
                        System.err.println("Input file must contain only one of the columns *datasetId, *pid and *databaseId");
                        return 1;
                    }
                    if (idColumnCount == 0) {
                        System.err.println("Input file must contain one of the columns *datasetId, *pid and *databaseId");
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

    private BatchProcessor.Result processRow(BatchProcessor.Row row, Map<String, String> defaultFieldValues, Map<String, DatasetFieldType> fieldSpecs) throws Exception {
        var effectiveValues = applyDefaults(row);
        var idColumns = ID_COLUMNS.stream().filter(column -> trimToNull(effectiveValues.get(column)) != null).toList();
        if (idColumns.isEmpty()) {
            return BatchProcessor.Result.failed("Missing dataset identifier: one of *datasetId, *pid or *databaseId is required");
        }
        if (idColumns.size() > 1) {
            return BatchProcessor.Result.failed("Only one of *datasetId, *pid and *databaseId may be specified");
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
        var expectedStateValue = trimToNull(effectiveValues.get(EXPECTED_STATE));
        var expectedStateMatcher = ExpectedState.parse(expectedStateValue == null ? "not deaccessioned" : expectedStateValue);
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

        var mismatch = findExpectedValueMismatch(effectiveValues, latestVersion, fieldSpecs);
        if (mismatch != null) {
            log.info("Skipping dataset {} because {}", id, mismatch);
            return BatchProcessor.Result.skipped(mismatch);
        }

        var editableValues = extractEditableValues(effectiveValues);
        var replaceExistingValues = parseBoolean(trimToNull(effectiveValues.get(REPLACE)), false, REPLACE);
        var fieldList = toFieldList(editableValues, fieldSpecs, replaceExistingValues, skipCode);
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
        var draftBeforeEdit = "DRAFT".equalsIgnoreCase(actualState);
        if (publicationMode != PublishVersion.LEAVE_DRAFT && draftBeforeEdit && expectedStateValue == null) {
            log.info("Not publishing dataset {} because it was a draft and expectedState was not explicitly provided", id);
        }
        else if (publicationMode != PublishVersion.LEAVE_DRAFT) {
            log.info("Publishing dataset {} as {}", id, publicationMode);
            datasetApi.publish(publicationMode.toUpdateType(), false);
            datasetApi.awaitState("RELEASED", timeout.toMilliseconds(), PUBLISH_POLL_INTERVAL_MS);
        }

        log.info("Finished metadata edit for dataset {}", id);
        return BatchProcessor.Result.ok("Metadata edited");
    }

    /**
     * Checks the columns named {@code =<fieldId>}. A blank cell means the field is expected to be empty. Returns a description of the first mismatch, or null if all expectations
     * hold.
     */
    private String findExpectedValueMismatch(Map<String, String> effectiveValues, DatasetVersion version, Map<String, DatasetFieldType> fieldSpecs) {
        Map<String, String> actualValues = null;
        for (var entry : effectiveValues.entrySet()) {
            if (!entry.getKey().startsWith(EXPECTED_VALUE_PREFIX)) {
                continue;
            }
            var expected = trimToNull(entry.getValue());
            var fieldId = entry.getKey().substring(EXPECTED_VALUE_PREFIX.length());
            var matcher = FIELD_PATTERN.matcher(fieldId);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid field column: " + entry.getKey());
            }
            var spec = getFieldSpecIgnoreCase(fieldSpecs, matcher.group("field"));
            if (spec == null) {
                throw new IllegalArgumentException("Unknown metadata field: " + matcher.group("field"));
            }
            if (actualValues == null) {
                actualValues = flattenFieldValues(version);
            }
            var key = spec.getName()
                + (matcher.group("index") != null ? "[" + Integer.parseInt(matcher.group("index")) + "]" : spec.isMultiple() ? "[1]" : "")
                + (matcher.group("subfield") != null ? "." + matcher.group("subfield") : "");
            var actual = actualValues.get(key.toLowerCase(Locale.ROOT));
            if (!Objects.equals(expected, actual)) {
                return String.format("Expected %s to be %s but found %s", fieldId, expected == null ? "empty" : "'" + expected + "'", actual == null ? "no value" : "'" + actual + "'");
            }
        }
        return null;
    }

    /**
     * Flattens the metadata of a version to column-style identifiers (lower case), e.g., {@code title}, {@code subject[1]}, {@code author[2].authorname}.
     */
    private static Map<String, String> flattenFieldValues(DatasetVersion version) {
        var result = new LinkedHashMap<String, String>();
        if (version.getMetadataBlocks() == null) {
            return result;
        }
        for (var block : version.getMetadataBlocks().values()) {
            if (block.getFields() == null) {
                continue;
            }
            for (var field : block.getFields()) {
                var name = field.getTypeName();
                if (field instanceof PrimitiveSingleValueField f) {
                    putValue(result, name, f.getValue());
                }
                else if (field instanceof ControlledSingleValueField f) {
                    putValue(result, name, f.getValue());
                }
                else if (field instanceof PrimitiveMultiValueField f && f.getValue() != null) {
                    for (var i = 0; i < f.getValue().size(); i++) {
                        putValue(result, name + "[" + (i + 1) + "]", f.getValue().get(i));
                    }
                }
                else if (field instanceof ControlledMultiValueField f && f.getValue() != null) {
                    for (var i = 0; i < f.getValue().size(); i++) {
                        putValue(result, name + "[" + (i + 1) + "]", f.getValue().get(i));
                    }
                }
                else if (field instanceof CompoundSingleValueField f) {
                    putChildValues(result, name, f.getValue());
                }
                else if (field instanceof CompoundMultiValueField f && f.getValue() != null) {
                    for (var i = 0; i < f.getValue().size(); i++) {
                        putChildValues(result, name + "[" + (i + 1) + "]", f.getValue().get(i));
                    }
                }
            }
        }
        return result;
    }

    private static void putChildValues(Map<String, String> result, String prefix, Map<String, SingleValueField> children) {
        if (children == null) {
            return;
        }
        children.forEach((childName, child) -> {
            if (child instanceof PrimitiveSingleValueField f) {
                putValue(result, prefix + "." + childName, f.getValue());
            }
            else if (child instanceof ControlledSingleValueField f) {
                putValue(result, prefix + "." + childName, f.getValue());
            }
        });
    }

    private static void putValue(Map<String, String> result, String key, String value) {
        if (value != null) {
            result.put(key.toLowerCase(Locale.ROOT), value);
        }
    }

    private Map<String, String> createBatchDefaults(Map<String, String> defaultFieldValues) {
        var defaults = createSingleRow(defaultFieldValues);
        // replace is only a default if explicitly requested
        if (!replace) {
            defaults.remove(REPLACE);
        }
        return defaults;
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
        var names = row.asMap().keySet();
        row.asMap().forEach((name, value) -> {
            // In single-row mode only starred names are special; unstarred ones are field assignments from the command line
            var column = inputFile != null ? toSpecialColumn(name, names) : null;
            effectiveValues.put(column != null ? column : name, value);
        });
        if (inputFile == null) {
            applyDefault(effectiveValues, DATASET_ID, datasetId);
            applyDefault(effectiveValues, PID, pid);
            applyDefault(effectiveValues, DATABASE_ID, databaseId);
        }
        applyDefault(effectiveValues, EXPECTED_STATE, expectedState);
        applyDefault(effectiveValues, EXPECT_IN_REVIEW, expectInReview);
        applyDefault(effectiveValues, PUBLISH_VERSION, publishVersion);
        applyDefault(effectiveValues, VERSION_NOTE, versionNote);
        applyDefault(effectiveValues, REPLACE, Boolean.toString(replace));
        return effectiveValues;
    }

    /**
     * Returns the canonical special column name for a column, or null if it is a metadata field column. The star is optional: an unstarred name is only
     * special if the input has no starred column with the same name. Otherwise, it is a field with that name.
     */
    private static String toSpecialColumn(String name, Collection<String> allNames) {
        return MetadataColumns.toSpecialColumn(name, allNames, RESERVED_COLUMNS);
    }

    private void applyDefault(Map<String, String> effectiveValues, String column, String value) {
        if (value != null && isBlank(effectiveValues.get(column))) {
            effectiveValues.put(column, value);
        }
    }

    private Map<String, String> extractEditableValues(Map<String, String> effectiveValues) {
        var values = new LinkedHashMap<String, String>();
        for (var entry : effectiveValues.entrySet()) {
            if (entry.getKey().startsWith(EXPECTED_VALUE_PREFIX)) {
                continue;
            }
            if (!entry.getKey().startsWith(SPECIAL_COLUMN_PREFIX)) {
                values.put(entry.getKey(), entry.getValue());
            } else if (!containsIgnoreCase(RESERVED_COLUMNS, entry.getKey())) {
                throw new IllegalArgumentException("Unknown special column: " + entry.getKey());
            }
        }
        return values;
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
    static boolean parseBoolean(String value, boolean defaultValue, String optionName) {
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
