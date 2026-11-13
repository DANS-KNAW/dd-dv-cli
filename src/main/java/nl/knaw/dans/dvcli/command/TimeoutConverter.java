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
import picocli.CommandLine.ITypeConverter;

/**
 * Converts a duration such as 30s, 2m or 1h; a value without a unit is interpreted as seconds.
 */
class TimeoutConverter implements ITypeConverter<Duration> {
    @Override
    public Duration convert(String value) {
        var trimmed = value.trim();
        return Duration.parse(trimmed.matches("\\d+") ? trimmed + "s" : trimmed);
    }
}
