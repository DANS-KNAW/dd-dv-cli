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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DatasetApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.model.Lock;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
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
    private static final Set<String> RESERVED_COLUMNS = Set.of(DATASET_ID, EXPECTED_STATE, EXPECT_IN_REVIEW, PUBLISH_VERSION, REPLACE);
    private static final Pattern FIELD_PATTERN = Pattern.compile("^([^\\.\\[]+)(?:\\[(\\d+)])?(?:\\.(.+))?$");
    private static final long PUBLISH_POLL_INTERVAL_MS = 5000L;

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

    @Parameters(arity = "0..*", paramLabel = "FIELD=VALUE", description = "Metadata field assignment defaults")
    private List<String> fieldAssignments = new ArrayList<>();

    private final DataverseClient dataverseClient;
    private final MetadataFieldSpecProvider metadataFieldSpecProvider;

    public DatasetEditMetadata(DataverseClient dataverseClient, URI baseUrl, String apiToken) {
        this(dataverseClient, new DataverseMetadataFieldSpecProvider(baseUrl, apiToken, HttpClient.newHttpClient(), new ObjectMapper()));
    }

    DatasetEditMetadata(DataverseClient dataverseClient, MetadataFieldSpecProvider metadataFieldSpecProvider) {
        this.dataverseClient = dataverseClient;
        this.metadataFieldSpecProvider = metadataFieldSpecProvider;
    }

    @Override
    public Integer call() {
        try {
            if (inputFile == null && isBlank(datasetId)) {
                System.err.println("Either --input-file or --datasetId is required");
                return 1;
            }

            var defaultFieldValues = parseAssignments(fieldAssignments);
            if (inputFile == null && defaultFieldValues.isEmpty()) {
                System.err.println("At least one metadata field assignment is required");
                return 1;
            }

            var fieldSpecs = metadataFieldSpecProvider.getFieldSpecs();
            try (var out = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
                var batchProcessor = inputFile != null
                    ? BatchProcessor.forCsv(inputFile, out)
                    : BatchProcessor.forSingleRow(createSingleRow(defaultFieldValues), out)) {

                var summary = batchProcessor.process(row -> processRow(row, defaultFieldValues, fieldSpecs));
                log.info("Finished: {} ok, {} failed, {} skipped", summary.getOkCount(), summary.getFailedCount(), summary.getSkippedCount());
                return summary.getFailedCount() == 0 ? 0 : 1;
            }
        }
        catch (Exception e) {
            System.err.println("Error editing dataset metadata: " + e.getMessage());
            return 1;
        }
    }

    private BatchProcessor.Result processRow(BatchProcessor.Row row, Map<String, String> defaultFieldValues, Map<String, MetadataFieldSpec> fieldSpecs) throws Exception {
        var effectiveValues = applyDefaults(row, defaultFieldValues);
        var id = trimToNull(effectiveValues.get(DATASET_ID));
        if (id == null) {
            return BatchProcessor.Result.failed("Missing datasetId");
        }

        var datasetApi = getDatasetApi(id);
        var latestVersion = datasetApi.getLatestVersion().getData().getLatestVersion();
        if (latestVersion == null || latestVersion.getVersionState() == null) {
            return BatchProcessor.Result.failed("Could not determine latest version state");
        }

        var actualState = latestVersion.getVersionState();
        var expectedStateMatcher = ExpectedState.parse(trimToNull(effectiveValues.get(EXPECTED_STATE)));
        if (!expectedStateMatcher.matches(actualState)) {
            return BatchProcessor.Result.skipped(String.format("Expected state %s but found %s", expectedStateMatcher.describe(), actualState));
        }

        var reviewExpectation = ReviewExpectation.parse(trimToNull(effectiveValues.get(EXPECT_IN_REVIEW)));
        var inReview = isInReview(datasetApi.getLocks().getData());
        if (!reviewExpectation.matches(inReview)) {
            return BatchProcessor.Result.skipped(String.format("Expected in review %s but found %s", reviewExpectation.describe(), inReview ? "yes" : "no"));
        }

        var editableValues = extractEditableValues(effectiveValues);
        if (editableValues.isEmpty()) {
            return BatchProcessor.Result.failed("No metadata fields specified");
        }

        var fieldList = toFieldList(editableValues, fieldSpecs);
        var replaceExistingValues = parseBoolean(trimToNull(effectiveValues.get(REPLACE)), false, REPLACE);
        datasetApi.editMetadata(fieldList, replaceExistingValues);

        var publicationMode = PublishVersion.parse(trimToNull(effectiveValues.get(PUBLISH_VERSION)));
        if (publicationMode != PublishVersion.LEAVE_DRAFT) {
            datasetApi.publish(publicationMode.toUpdateType(), false);
            datasetApi.awaitState("RELEASED", Duration.ofMinutes(timeoutInMinutes).toMillis(), PUBLISH_POLL_INTERVAL_MS);
        }

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
            parsed.put(assignment.substring(0, separator), assignment.substring(separator + 1));
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

    private FieldList toFieldList(Map<String, String> editableValues, Map<String, MetadataFieldSpec> fieldSpecs) {
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

    private int determineIndex(MetadataFieldSpec spec, String indexText, String columnName) {
        if (indexText == null) {
            return spec.isMultiple() ? 1 : 0;
        }

        if (!spec.isMultiple()) {
            throw new IllegalArgumentException("Field does not allow indexed values: " + columnName);
        }
        return Integer.parseInt(indexText);
    }

    private boolean isInReview(List<Lock> locks) {
        if (locks == null) {
            return false;
        }
        return locks.stream()
            .map(Lock::getLockType)
            .filter(Objects::nonNull)
            .anyMatch(lockType -> "InReview".equalsIgnoreCase(lockType));
    }

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
        Map<String, MetadataFieldSpec> getFieldSpecs() throws Exception;
    }

    static class DataverseMetadataFieldSpecProvider implements MetadataFieldSpecProvider {
        private final URI baseUrl;
        private final String apiToken;
        private final HttpClient httpClient;
        private final ObjectMapper objectMapper;
        private Map<String, MetadataFieldSpec> cachedFieldSpecs;

        DataverseMetadataFieldSpecProvider(URI baseUrl, String apiToken, HttpClient httpClient, ObjectMapper objectMapper) {
            this.baseUrl = baseUrl;
            this.apiToken = apiToken;
            this.httpClient = httpClient;
            this.objectMapper = objectMapper;
        }

        @Override
        public Map<String, MetadataFieldSpec> getFieldSpecs() throws Exception {
            if (cachedFieldSpecs != null) {
                return cachedFieldSpecs;
            }

            var requestBuilder = HttpRequest.newBuilder(baseUrl.resolve("api/metadatablocks?returnDatasetFieldTypes=true"))
                .GET();
            if (apiToken != null) {
                requestBuilder.header("X-Dataverse-key", apiToken);
            }

            var response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IOException("Failed to load metadata field definitions: HTTP " + response.statusCode());
            }

            var root = objectMapper.readTree(response.body());
            var specs = new LinkedHashMap<String, MetadataFieldSpec>();
            for (JsonNode blockNode : root.path("data")) {
                var fieldsNode = blockNode.path("fields");
                if (!fieldsNode.isObject()) {
                    continue;
                }
                var iterator = fieldsNode.fields();
                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    specs.put(entry.getKey(), parseFieldSpec(entry.getKey(), entry.getValue()));
                }
            }
            cachedFieldSpecs = Collections.unmodifiableMap(specs);
            return cachedFieldSpecs;
        }

        private MetadataFieldSpec parseFieldSpec(String fieldName, JsonNode node) {
            var childFields = new LinkedHashMap<String, MetadataFieldSpec>();
            var childFieldsNode = node.path("childFields");
            if (childFieldsNode.isObject()) {
                var childIterator = childFieldsNode.fields();
                while (childIterator.hasNext()) {
                    var child = childIterator.next();
                    childFields.put(child.getKey(), parseFieldSpec(child.getKey(), child.getValue()));
                }
            }

            var vocabularyValues = new LinkedHashSet<String>();
            var vocabularyNode = node.path("controlledVocabularyValues");
            if (vocabularyNode.isArray()) {
                for (JsonNode valueNode : vocabularyNode) {
                    vocabularyValues.add(valueNode.asText());
                }
            }

            return new MetadataFieldSpec(
                fieldName,
                node.path("typeClass").asText(),
                node.path("multiple").asBoolean(false),
                childFields,
                vocabularyValues
            );
        }
    }

    static class MetadataFieldSpec {
        private final String typeName;
        private final String typeClass;
        private final boolean multiple;
        private final Map<String, MetadataFieldSpec> childFields;
        private final Set<String> controlledVocabularyValues;

        MetadataFieldSpec(String typeName, String typeClass, boolean multiple, Map<String, MetadataFieldSpec> childFields, Collection<String> controlledVocabularyValues) {
            this.typeName = typeName;
            this.typeClass = typeClass;
            this.multiple = multiple;
            this.childFields = Collections.unmodifiableMap(new LinkedHashMap<>(childFields));
            this.controlledVocabularyValues = Collections.unmodifiableSet(new LinkedHashSet<>(controlledVocabularyValues));
        }

        public String getTypeName() {
            return typeName;
        }

        public String getTypeClass() {
            return typeClass;
        }

        public boolean isMultiple() {
            return multiple;
        }

        public Map<String, MetadataFieldSpec> getChildFields() {
            return childFields;
        }

        public Set<String> getControlledVocabularyValues() {
            return controlledVocabularyValues;
        }
    }

    static class FieldValueAccumulator {
        private final MetadataFieldSpec fieldSpec;
        private final Map<Integer, String> simpleValues = new TreeMap<>();
        private final Map<Integer, Map<String, String>> compoundValues = new TreeMap<>();

        FieldValueAccumulator(MetadataFieldSpec fieldSpec) {
            this.fieldSpec = fieldSpec;
        }

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

            var subfieldSpec = fieldSpec.getChildFields().get(subfieldName);
            if (subfieldSpec == null) {
                throw new IllegalArgumentException("Unknown subfield " + subfieldName + " for " + fieldSpec.getTypeName());
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
                throw new IllegalArgumentException("No value provided for " + fieldSpec.getTypeName());
            }

            if (!fieldSpec.isMultiple()) {
                if (simpleValues.size() > 1) {
                    throw new IllegalArgumentException("Field does not allow multiple values: " + fieldSpec.getTypeName());
                }
                return (MetadataField) createSingleValueField(fieldSpec, simpleValues.values().iterator().next());
            }

            var values = new ArrayList<String>();
            for (String value : simpleValues.values()) {
                validateControlledVocabulary(fieldSpec, value);
                values.add(value);
            }

            return switch (fieldSpec.getTypeClass()) {
                case "primitive" -> new PrimitiveMultiValueField(fieldSpec.getTypeName(), values);
                case "controlledVocabulary" -> new ControlledMultiValueField(fieldSpec.getTypeName(), values);
                default -> throw new IllegalArgumentException("Unsupported field typeClass for " + fieldSpec.getTypeName() + ": " + fieldSpec.getTypeClass());
            };
        }

        private MetadataField toCompoundField() {
            if (compoundValues.isEmpty()) {
                throw new IllegalArgumentException("No value provided for " + fieldSpec.getTypeName());
            }

            if (!fieldSpec.isMultiple()) {
                if (compoundValues.size() > 1) {
                    throw new IllegalArgumentException("Field does not allow multiple values: " + fieldSpec.getTypeName());
                }
                return new CompoundSingleValueField(fieldSpec.getTypeName(), createCompoundValue(compoundValues.values().iterator().next()));
            }

            var values = new ArrayList<Map<String, SingleValueField>>();
            for (Map<String, String> compoundValue : compoundValues.values()) {
                values.add(createCompoundValue(compoundValue));
            }
            return new CompoundMultiValueField(fieldSpec.getTypeName(), values);
        }

        private Map<String, SingleValueField> createCompoundValue(Map<String, String> compoundValue) {
            var values = new LinkedHashMap<String, SingleValueField>();
            for (var entry : compoundValue.entrySet()) {
                var subfieldSpec = fieldSpec.getChildFields().get(entry.getKey());
                values.put(entry.getKey(), createSingleValueField(subfieldSpec, entry.getValue()));
            }
            return values;
        }

        private SingleValueField createSingleValueField(MetadataFieldSpec spec, String value) {
            validateControlledVocabulary(spec, value);

            return switch (spec.getTypeClass()) {
                case "primitive" -> new PrimitiveSingleValueField(spec.getTypeName(), value);
                case "controlledVocabulary" -> new ControlledSingleValueField(spec.getTypeName(), value);
                default -> throw new IllegalArgumentException("Unsupported field typeClass for " + spec.getTypeName() + ": " + spec.getTypeClass());
            };
        }

        private void validateControlledVocabulary(MetadataFieldSpec spec, String value) {
            if (!"controlledVocabulary".equals(spec.getTypeClass()) || spec.getControlledVocabularyValues().isEmpty()) {
                return;
            }
            if (!spec.getControlledVocabularyValues().contains(value)) {
                throw new IllegalArgumentException("Invalid controlled vocabulary value for " + spec.getTypeName() + ": " + value);
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
            return negated ? !matches : matches;
        }

        String describe() {
            return negated ? "not " + state : state;
        }
    }
}
