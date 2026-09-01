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

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class OffsetDateTimeConverterTest {

    private final OffsetDateTimeConverter converter = new OffsetDateTimeConverter();

    @Test
    public void full_iso_datetime_is_parsed_unchanged() throws Exception {
        OffsetDateTime result = converter.convert("2025-01-01T13:45:00+02:00");
        assertThat(result).isEqualTo(OffsetDateTime.parse("2025-01-01T13:45:00+02:00"));
    }

    @Test
    public void full_iso_datetime_with_z_is_parsed_unchanged() throws Exception {
        OffsetDateTime result = converter.convert("2025-06-15T00:00:00Z");
        assertThat(result).isEqualTo(OffsetDateTime.parse("2025-06-15T00:00:00Z"));
    }

    @Test
    public void date_only_is_extended_to_midnight_utc() throws Exception {
        OffsetDateTime result = converter.convert("2025-01-01");
        assertThat(result).isEqualTo(OffsetDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC));
    }

    @Test
    public void invalid_value_throws_exception() {
        assertThatThrownBy(() -> converter.convert("not-a-date"))
            .isInstanceOf(Exception.class);
    }
}
