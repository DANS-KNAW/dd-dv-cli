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
import picocli.CommandLine.TypeConversionException;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LocalTimestampConverterTest {
    private final LocalTimestampConverter converter = new LocalTimestampConverter();

    @Test
    public void converts_local_timestamp() {
        assertThat(converter.convert("2025-01-01T12:34:56"))
            .isEqualTo(LocalDateTime.of(2025, 1, 1, 12, 34, 56));
    }

    @Test
    public void converts_date_to_start_of_day() {
        assertThat(converter.convert("2025-01-01"))
            .isEqualTo(LocalDateTime.of(2025, 1, 1, 0, 0, 0));
    }

    @Test
    public void rejects_timezone_offsets() {
        assertThatThrownBy(() -> converter.convert("2025-01-01T12:34:56Z"))
            .isInstanceOf(TypeConversionException.class);
    }
}
