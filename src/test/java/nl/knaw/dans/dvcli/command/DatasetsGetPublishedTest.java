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
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetsGetPublishedTest {

    @Test
    public void archived_flag_generates_correct_query() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatasetsGetPublished cmd = new DatasetsGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        StringWriter sw = new StringWriter();
        commandLine.setOut(new PrintWriter(sw));
        commandLine.execute("--archived");

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(dbApi).query(queryCaptor.capture(), Mockito.any());
        String query = queryCaptor.getValue();
        assertThat(query).contains("dsv.archivalcopylocation::json ->> 'status' = 'success'");

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);
        
        // Expected order of params in fetchResults:
        // after, before, archived, unarchived, failedArchived, archived, unarchived, failedArchived, updateCurrent, updateCurrent
        assertThat(params[2]).isEqualTo(true);
        assertThat(params[3]).isEqualTo(false);
        assertThat(params[4]).isEqualTo(false);
        assertThat(params[5]).isEqualTo(true);
        assertThat(params[6]).isEqualTo(false);
        assertThat(params[7]).isEqualTo(false);
    }

    @Test
    public void unarchived_flag_generates_correct_query() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatasetsGetPublished cmd = new DatasetsGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        StringWriter sw = new StringWriter();
        commandLine.setOut(new PrintWriter(sw));
        commandLine.execute("--unarchived");

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(dbApi).query(queryCaptor.capture(), Mockito.any());
        String query = queryCaptor.getValue();
        assertThat(query).contains("(dsv.archivalcopylocation IS NULL OR dsv.archivalcopylocation::json ->> 'status' = 'failure')");

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);

        assertThat(params[2]).isEqualTo(false);
        assertThat(params[3]).isEqualTo(true);
        assertThat(params[4]).isEqualTo(false);
    }

    @Test
    public void failed_archived_flag_generates_correct_query() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatasetsGetPublished cmd = new DatasetsGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        StringWriter sw = new StringWriter();
        commandLine.setOut(new PrintWriter(sw));
        commandLine.execute("--failed-archived");

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(dbApi).query(queryCaptor.capture(), Mockito.any());
        String query = queryCaptor.getValue();
        assertThat(query).contains("(? = true AND dsv.archivalcopylocation IS NOT NULL AND dsv.archivalcopylocation::json ->> 'status' = 'failure')");

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);

        assertThat(params[2]).isEqualTo(false);
        assertThat(params[3]).isEqualTo(false);
        assertThat(params[4]).isEqualTo(true);
    }

    @Test
    public void no_flags_generates_correct_query() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatasetsGetPublished cmd = new DatasetsGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        StringWriter sw = new StringWriter();
        commandLine.setOut(new PrintWriter(sw));
        commandLine.execute();

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(dbApi).query(queryCaptor.capture(), Mockito.any());
        String query = queryCaptor.getValue();
        assertThat(query).contains("(? = false AND ? = false AND ? = false)");

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);

        assertThat(params[2]).isEqualTo(false);
        assertThat(params[3]).isEqualTo(false);
        assertThat(params[4]).isEqualTo(false);
    }

    @Test
    public void date_only_after_is_extended_to_midnight_utc() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatasetsGetPublished cmd = new DatasetsGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        commandLine.setOut(new PrintWriter(new StringWriter()));
        commandLine.execute("--after", "2024-06-15");

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);

        assertThat(params[0]).isEqualTo(Timestamp.from(OffsetDateTime.parse("2024-06-15T00:00:00Z").toInstant()));
    }

    @Test
    public void date_only_before_is_extended_to_midnight_utc() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatasetsGetPublished cmd = new DatasetsGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        commandLine.setOut(new PrintWriter(new StringWriter()));
        commandLine.execute("--before", "2025-03-20");

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);

        assertThat(params[1]).isEqualTo(Timestamp.from(OffsetDateTime.parse("2025-03-20T00:00:00Z").toInstant()));
    }
}
