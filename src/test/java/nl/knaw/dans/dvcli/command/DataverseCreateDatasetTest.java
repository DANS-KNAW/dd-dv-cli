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
import nl.knaw.dans.lib.dataverse.DataverseApi;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseHttpResponse;
import nl.knaw.dans.lib.dataverse.model.dataset.Dataset;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetCreationResult;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetFieldType;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlockDefinition;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.UpdateType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DataverseCreateDatasetTest {

    @Test
    void publish_parse_accepts_yes_and_no_synonyms() {
        assertThat(DataverseCreateDataset.PublishChoice.parse(null)).isEqualTo(DataverseCreateDataset.PublishChoice.NO);
        assertThat(DataverseCreateDataset.PublishChoice.parse("leave-draft")).isEqualTo(DataverseCreateDataset.PublishChoice.NO);
        assertThat(DataverseCreateDataset.PublishChoice.parse("YES")).isEqualTo(DataverseCreateDataset.PublishChoice.YES);
    }

    @Test
    void creates_dataset_in_given_collection_without_publishing() throws Exception {
        var setup = new Setup();

        var result = setup.run("--alias", "mycoll", "title=My title");

        assertThat(result.exitCode).isZero();
        assertThat(result.stdout).contains("Created doi:10.5072/FK2/NEW");
        var captor = ArgumentCaptor.forClass(Dataset.class);
        Mockito.verify(setup.dataverseApi).createDataset(captor.capture());
        assertThat(captor.getValue().getDatasetVersion().getFiles()).isEmpty();
        var block = captor.getValue().getDatasetVersion().getMetadataBlocks().get("citation");
        assertThat(block.getFields()).hasSize(1);
        assertThat(((PrimitiveSingleValueField) block.getFields().get(0)).getValue()).isEqualTo("My title");
        Mockito.verify(setup.dataverseClient).dataverse("mycoll");
        Mockito.verify(setup.datasetApi, Mockito.never()).publish(Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    void publishes_as_major_version_when_requested() throws Exception {
        var setup = new Setup();

        var result = setup.run("--publish", "yes", "title=My title");

        assertThat(result.exitCode).isZero();
        assertThat(result.stdout).contains("Created and published doi:10.5072/FK2/NEW");
        Mockito.verify(setup.dataverseClient).dataverse("root");
        Mockito.verify(setup.datasetApi).publish(UpdateType.major, false);
        Mockito.verify(setup.datasetApi).awaitState(Mockito.eq("RELEASED"), Mockito.anyLong(), Mockito.anyLong());
    }

    @Test
    void rejects_expected_values_and_missing_fields() {
        var setup = new Setup();

        assertThat(setup.run("=title=Old", "title=New").exitCode).isEqualTo(1);
        assertThat(setup.run().exitCode).isEqualTo(1);
        assertThat(setup.run("--publish", "major", "title=x").exitCode).isEqualTo(1);
    }

    @Test
    void batch_mode_uses_alias_and_publish_columns() throws Exception {
        var setup = new Setup();
        var csv = Files.createTempFile("create", ".csv");
        Files.writeString(csv, "alias,publish,title\ncoll1,yes,First\n,no,Second\n");

        var result = setup.run("-i", csv.toString());

        assertThat(result.exitCode).isZero();
        Mockito.verify(setup.dataverseClient).dataverse("coll1");
        Mockito.verify(setup.dataverseClient).dataverse("root");
        Mockito.verify(setup.datasetApi, Mockito.times(1)).publish(UpdateType.major, false);
        Files.delete(csv);
    }

    private static class Setup {
        final DataverseClient dataverseClient = Mockito.mock(DataverseClient.class);
        final DataverseApi dataverseApi = Mockito.mock(DataverseApi.class);
        final DatasetApi datasetApi = Mockito.mock(DatasetApi.class);

        @SuppressWarnings("unchecked")
        Setup() {
            try {
                var created = new DatasetCreationResult();
                created.setId(1);
                created.setPersistentId("doi:10.5072/FK2/NEW");
                var response = Mockito.mock(DataverseHttpResponse.class);
                Mockito.doReturn(created).when(response).getData();
                Mockito.when(dataverseClient.dataverse(Mockito.anyString())).thenReturn(dataverseApi);
                Mockito.when(dataverseApi.createDataset(Mockito.any(Dataset.class))).thenReturn(response);
                Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/NEW")).thenReturn(datasetApi);
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        Result run(String... args) {
            var title = new DatasetFieldType();
            title.setName("title");
            title.setTypeClass("primitive");
            var block = new MetadataBlockDefinition();
            block.setName("citation");
            block.setDisplayName("Citation Metadata");
            block.setFields(Map.of("title", title));
            var command = new CommandLine(new DataverseCreateDataset(dataverseClient, alias -> List.of(block), null));

            var originalOut = System.out;
            var originalErr = System.err;
            var out = new ByteArrayOutputStream();
            try {
                System.setOut(new PrintStream(out, true));
                System.setErr(new PrintStream(new ByteArrayOutputStream(), true));
                return new Result(command.execute(args), out.toString());
            }
            finally {
                System.setOut(originalOut);
                System.setErr(originalErr);
            }
        }
    }

    private record Result(int exitCode, String stdout) {
    }
}
