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

import nl.knaw.dans.lib.dataverse.model.dataset.DatasetFieldType;
import nl.knaw.dans.lib.dataverse.model.dataset.FieldList;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static nl.knaw.dans.dvcli.command.Strings.trimToNull;

/**
 * Converts column name / value pairs (e.g. {@code author[2].authorName=Doe}) into a Dataverse {@link FieldList}, using the field definitions.
 */
final class DatasetFieldConverter {
    static final Pattern FIELD_PATTERN = Pattern.compile("^(?<field>[^.\\[]+)(?:\\[(?<index>\\d+)])?(?:\\.(?<subfield>.+))?$");

    private DatasetFieldConverter() {
    }

    static FieldList toFieldList(Map<String, String> editableValues, Map<String, DatasetFieldType> fieldSpecs, boolean replaceExistingValues, String skipCode) {
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

    static DatasetFieldType getFieldSpecIgnoreCase(Map<String, DatasetFieldType> specs, String name) {
        if (specs == null) {
            return null;
        }
        return specs.entrySet().stream()
            .filter(entry -> entry.getKey().equalsIgnoreCase(name))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
    }

    private static void markSkipped(String columnName, Map<String, DatasetFieldType> fieldSpecs, Map<String, FieldValueAccumulator> accumulators) {
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

    private static int determineIndex(DatasetFieldType spec, String indexText, String columnName) {
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
}
