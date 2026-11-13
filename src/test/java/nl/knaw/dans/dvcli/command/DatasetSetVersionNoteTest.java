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

import nl.knaw.dans.lib.dataverse.DatasetApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetSetVersionNoteTest {

    @Test
    void sets_note_on_draft_version_by_default() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        Mockito.when(client.dataset("doi:10.5072/FK2/ABC")).thenReturn(api);

        assertThat(new CommandLine(new DatasetSetVersionNote(client)).execute("doi:10.5072/FK2/ABC", "My note")).isZero();

        Mockito.verify(api).setVersionNote(":draft", "My note");
    }

    @Test
    void sets_note_on_specified_version() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        Mockito.when(client.dataset(5)).thenReturn(api);

        assertThat(new CommandLine(new DatasetSetVersionNote(client)).execute("--version", ":latest-published", "5", "Note")).isZero();

        Mockito.verify(api).setVersionNote(":latest-published", "Note");
    }
}
