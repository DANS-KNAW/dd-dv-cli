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
import nl.knaw.dans.lib.dataverse.model.dataset.UpdateType;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetPublishTest {

    @Test
    void publishes_minor_by_pid_by_default() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        Mockito.when(client.dataset("doi:10.5072/FK2/ABC")).thenReturn(api);

        assertThat(new CommandLine(new DatasetPublish(client)).execute("doi:10.5072/FK2/ABC")).isZero();

        Mockito.verify(api).publish(UpdateType.minor, false);
        Mockito.verify(api, Mockito.never()).setVersionNote(Mockito.any());
    }

    @Test
    void digits_are_database_id_and_version_note_is_set_first() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        Mockito.when(client.dataset(42)).thenReturn(api);

        assertThat(new CommandLine(new DatasetPublish(client)).execute("--major", "--versionNote", "Fixed typo", "42")).isZero();

        InOrder inOrder = Mockito.inOrder(api);
        inOrder.verify(api).setVersionNote("Fixed typo");
        inOrder.verify(api).publish(UpdateType.major, false);
    }

    @Test
    void updatecurrent_is_supported_and_options_are_mutually_exclusive() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        Mockito.when(client.dataset(7)).thenReturn(api);

        assertThat(new CommandLine(new DatasetPublish(client)).execute("--updatecurrent", "7")).isZero();
        Mockito.verify(api).publish(UpdateType.updatecurrent, false);
        assertThat(new CommandLine(new DatasetPublish(client)).execute("--major", "--minor", "7")).isEqualTo(1);
    }
}
