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
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FilemetasGetPublishedTest {

    @Test
    public void query_uses_latest_released_dataset_versions(@TempDir Path tempDir) throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(List.of());

        FilemetasGetPublished cmd = new FilemetasGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        commandLine.execute("--output", tempDir.resolve("out.csv").toString());

        ArgumentCaptor<String> queryCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(dbApi).query(queryCaptor.capture(), Mockito.any());
        String query = queryCaptor.getValue();

        assertThat(query).contains("WITH latest_published AS");
        assertThat(query).contains("WHERE dv.versionstate = 'RELEASED'");
        assertThat(query).contains("JOIN filemetadata fm ON fm.datasetversion_id = lp.datasetversion_id");
    }

    @Test
    public void csv_contains_requested_optional_columns(@TempDir Path tempDir) throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(List.of(
            new FilemetasGetPublished.FileMetadataInfo(101L, "doi:10.5072/FK2/ABC123", "example.txt", "dir/subdir")
        ));

        Path outputFile = tempDir.resolve("out.csv");
        FilemetasGetPublished cmd = new FilemetasGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        commandLine.execute(
            "--output", outputFile.toString(),
            "--dataset-pid",
            "--label",
            "--directory-label"
        );

        assertThat(Files.readString(outputFile))
            .isEqualToNormalizingNewlines("""
                FILEID,DATASET_PID,LABEL,DIRECTORY_LABEL
                101,doi:10.5072/FK2/ABC123,example.txt,dir/subdir
                """);
    }

    @Test
    public void csv_contains_only_fileid_by_default(@TempDir Path tempDir) throws Exception {
        DatabaseApi dbApi = Mockito.mock(DatabaseApi.class);
        QueryContext queryContext = Mockito.mock(QueryContext.class);
        Mockito.when(dbApi.query(Mockito.anyString(), Mockito.any())).thenReturn(queryContext);
        Mockito.when(queryContext.executeFor(Mockito.any())).thenReturn(List.of(
            new FilemetasGetPublished.FileMetadataInfo(101L, "doi:10.5072/FK2/ABC123", "example.txt", "dir/subdir")
        ));

        Path outputFile = tempDir.resolve("out.csv");
        FilemetasGetPublished cmd = new FilemetasGetPublished(dbApi);
        CommandLine commandLine = new CommandLine(cmd);
        commandLine.execute("--output", outputFile.toString());

        assertThat(Files.readString(outputFile))
            .isEqualToNormalizingNewlines("""
                FILEID
                101
                """);
    }
}
