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

import lombok.RequiredArgsConstructor;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetFieldType;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.SingleValueField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static nl.knaw.dans.dvcli.command.DatasetFieldConverter.getFieldSpecIgnoreCase;

/**
 * Collects the values for one metadata field, spread over one or more columns, and turns them into a {@link MetadataField}.
 */
@RequiredArgsConstructor
class FieldValueAccumulator {
    private final DatasetFieldType fieldSpec;
    private final Map<Integer, String> simpleValues = new TreeMap<>();
    private final Map<Integer, Map<String, String>> compoundValues = new TreeMap<>();
    private boolean blank;
    private boolean skipped;

    void addValue(int index, String value, String columnName) {
        checkNotSkipped(columnName);
        if ("compound".equals(fieldSpec.getTypeClass())) {
            throw new IllegalArgumentException("Compound field requires subfields: " + columnName);
        }
        if (simpleValues.putIfAbsent(index, value) != null) {
            throw new IllegalArgumentException("Duplicate field value for " + columnName);
        }
    }

    void addSubfieldValue(int index, String subfieldName, String value, String columnName) {
        checkNotSkipped(columnName);
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
        if (!simpleValues.isEmpty() || !compoundValues.isEmpty()) {
            throw new IllegalArgumentException("Cannot mix skip code and other values in field " + fieldSpec.getName());
        }
        skipped = true;
    }

    private void checkNotSkipped(String columnName) {
        if (skipped) {
            throw new IllegalArgumentException("Cannot mix skip code and other values in field " + fieldSpec.getName() + " (column " + columnName + ")");
        }
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
