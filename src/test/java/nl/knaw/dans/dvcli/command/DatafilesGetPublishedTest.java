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

import nl.knaw.dans.lib.dataverse.DatabaseApi;
import nl.knaw.dans.lib.dataverse.QueryContext;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DatafilesGetPublishedTest {

    @Test
    public void before_flag_generates_correct_query() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatafilesGetPublished cmd = new DatafilesGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        StringWriter sw = new StringWriter();
        commandLine.setOut(new PrintWriter(sw));
        var outputFile = Files.createTempFile("datafiles-get-published", ".csv");
        var before = "2025-01-01T00:00:00Z";
        commandLine.execute("-o", outputFile.toString(), "--before", before);

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(dbApi).query(queryCaptor.capture(), Mockito.any());
        String query = queryCaptor.getValue();
        assertThat(query).contains("dvo.publicationdate > ?");
        assertThat(query).contains("dvo.publicationdate < ?");

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);

        assertThat(params[0]).isEqualTo(Timestamp.from(OffsetDateTime.parse("1970-01-01T00:00:00Z").toInstant()));
        assertThat(params[1]).isEqualTo(Timestamp.from(OffsetDateTime.parse(before).toInstant()));
    }

    @Test
    public void after_and_before_flags_generate_correct_parameters() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatafilesGetPublished cmd = new DatafilesGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        StringWriter sw = new StringWriter();
        commandLine.setOut(new PrintWriter(sw));
        var outputFile = Files.createTempFile("datafiles-get-published", ".csv");
        var after = "2024-01-01T00:00:00Z";
        var before = "2025-01-01T00:00:00Z";
        commandLine.execute("-o", outputFile.toString(), "--after", after, "--before", before);

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);

        assertThat(params[0]).isEqualTo(Timestamp.from(OffsetDateTime.parse(after).toInstant()));
        assertThat(params[1]).isEqualTo(Timestamp.from(OffsetDateTime.parse(before).toInstant()));
    }
}
