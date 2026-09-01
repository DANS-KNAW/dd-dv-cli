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
import picocli.CommandLine.TypeConversionException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

public class LocalTimestampConverter implements ITypeConverter<LocalDateTime> {
    @Override
    public LocalDateTime convert(String value) {
        try {
            return LocalDateTime.parse(value);
        }
        catch (DateTimeParseException e) {
            try {
                return LocalDate.parse(value).atStartOfDay();
            }
            catch (DateTimeParseException ignored) {
                throw new TypeConversionException("'" + value + "': expected an ISO local timestamp without timezone or a date (e.g. 2025-01-01T00:00:00 or 2025-01-01)");
            }
        }
    }
}
