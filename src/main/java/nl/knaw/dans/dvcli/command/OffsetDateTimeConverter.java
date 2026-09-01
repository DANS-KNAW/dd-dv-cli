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

import picocli.CommandLine.ITypeConverter;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

/**
 * Converts a string to an {@link OffsetDateTime}. Accepts either a full ISO-8601 date-time string
 * (e.g. {@code 2025-01-01T00:00:00+01:00}) or a plain date string (e.g. {@code 2025-01-01}), in
 * which case the time component is implicitly set to {@code 00:00:00Z}.
 */
public class OffsetDateTimeConverter implements ITypeConverter<OffsetDateTime> {

    @Override
    public OffsetDateTime convert(String value) throws Exception {
        try {
            return OffsetDateTime.parse(value);
        }
        catch (DateTimeParseException e) {
            // Try parsing as a plain date; extend with 00:00:00Z
            LocalDate date = LocalDate.parse(value);
            return date.atStartOfDay().atOffset(ZoneOffset.UTC);
        }
    }
}
