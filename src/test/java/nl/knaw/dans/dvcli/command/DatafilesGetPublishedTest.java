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

import java.io.File;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DatafilesGetPublishedTest {
    @Test
    public void after_accepts_date_only_and_uses_start_of_day_timestamp() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatafilesGetPublished cmd = new DatafilesGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        commandLine.execute("--output", new File("out.csv").getAbsolutePath(), "--after", "2025-01-01");

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);

        assertThat(params[0]).isEqualTo(Timestamp.valueOf("2025-01-01 00:00:00"));
    }

    @Test
    public void after_accepts_timestamp_without_timezone() throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(Collections.emptyList());

        DatafilesGetPublished cmd = new DatafilesGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        commandLine.execute("--output", new File("out.csv").getAbsolutePath(), "--after", "2025-01-01T12:34:56");

        ArgumentCaptor<List<Object[]>> paramsCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(queryContext).executeFor(paramsCaptor.capture());
        Object[] params = paramsCaptor.getValue().get(0);

        assertThat(params[0]).isEqualTo(Timestamp.valueOf("2025-01-01 12:34:56"));
    }
}
