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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Conventions for the columns / assignments used by the metadata commands: special (starred) columns, expected-value columns and FIELD=VALUE assignments.
 */
final class MetadataColumns {
    static final String SPECIAL_COLUMN_PREFIX = "*";
    static final String EXPECTED_VALUE_PREFIX = "=";

    private MetadataColumns() {
    }

    /**
     * Returns the canonical special column name for a column, or null if it is a metadata field column. The star is optional: an unstarred name is only
     * special if the input has no starred column with the same name. Otherwise, it is a field with that name.
     */
    static String toSpecialColumn(String name, Collection<String> allNames, Set<String> reservedColumns) {
        if (name.startsWith(SPECIAL_COLUMN_PREFIX)) {
            return reservedColumns.stream().filter(name::equalsIgnoreCase).findFirst().orElse(name);
        }
        var starred = SPECIAL_COLUMN_PREFIX + name;
        var special = reservedColumns.stream().filter(starred::equalsIgnoreCase).findFirst();
        if (special.isPresent() && allNames.stream().noneMatch(starred::equalsIgnoreCase)) {
            return special.get();
        }
        return null;
    }

    static Map<String, String> parseAssignments(List<String> assignments) {
        var parsed = new LinkedHashMap<String, String>();
        for (String assignment : assignments) {
            if (assignment.isBlank()) {
                continue;
            }

            // A leading '=' marks an expected-value column, so the separator is the next '='
            int separator = assignment.indexOf('=', assignment.startsWith(EXPECTED_VALUE_PREFIX) ? 1 : 0);
            if (separator <= 0 || separator == 1 && assignment.startsWith(EXPECTED_VALUE_PREFIX)) {
                throw new IllegalArgumentException("Invalid field assignment: " + assignment);
            }
            String fieldName = assignment.substring(0, separator);
            if (fieldName.startsWith(SPECIAL_COLUMN_PREFIX)) {
                throw new IllegalArgumentException("Field name must not start with " + SPECIAL_COLUMN_PREFIX + ": " + fieldName);
            }
            parsed.put(fieldName, assignment.substring(separator + 1));
        }
        return parsed;
    }

    static boolean containsIgnoreCase(Set<String> names, String name) {
        return names.stream().anyMatch(name::equalsIgnoreCase);
    }
}
