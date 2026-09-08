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
import nl.knaw.dans.lib.dataverse.DataverseHttpResponse;
import nl.knaw.dans.lib.dataverse.MetadataBlocksApi;
import nl.knaw.dans.lib.dataverse.Version;
import nl.knaw.dans.lib.dataverse.model.DataMessage;
import nl.knaw.dans.lib.dataverse.model.Lock;
import nl.knaw.dans.lib.dataverse.model.dataset.CompoundMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.ControlledMultiValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetFieldType;
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion;
import nl.knaw.dans.lib.dataverse.model.dataset.FieldList;
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlockDefinition;
import nl.knaw.dans.lib.dataverse.model.dataset.PrimitiveSingleValueField;
import nl.knaw.dans.lib.dataverse.model.dataset.UpdateType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetEditMetadataTest {
    @TempDir
    Path tempDir;

    @Test
    void dataset_edit_metadata_calls_edit_endpoint() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var fieldSpecs = createFieldSpecs();
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(mockDatasetVersionResponse());

        var exitCode = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, () -> fieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "title=New title",
            "subject=Chemistry",
            "author.authorName=Alice"
        ).exitCode();

        assertThat(exitCode).isZero();

        var captor = ArgumentCaptor.forClass(FieldList.class);
        Mockito.verify(datasetApi).editMetadata(captor.capture(), Mockito.eq(false));

        var fields = captor.getValue().getFields();
        assertThat(fields).hasSize(3);
        assertThat(fields.get(0)).isInstanceOf(PrimitiveSingleValueField.class);
        assertThat(((PrimitiveSingleValueField) fields.get(0)).getValue()).isEqualTo("New title");
        assertThat(fields.get(1)).isInstanceOf(ControlledMultiValueField.class);
        assertThat(((ControlledMultiValueField) fields.get(1)).getValue()).containsExactly("Chemistry");
        assertThat(fields.get(2)).isInstanceOf(CompoundMultiValueField.class);
        assertThat(((CompoundMultiValueField) fields.get(2)).getValue()).hasSize(1);
    }

    @Test
    void dataset_edit_metadata_skips_when_default_expected_state_does_not_match() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var latestVersionResponse = mockLatestVersionResponse("DRAFT");
        var lockResponse = mockLockResponse(List.of());

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "title=New title"
        );

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("SKIPPED");
        Mockito.verify(datasetApi, Mockito.never()).editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean());
    }

    @Test
    void dataset_edit_metadata_uses_cli_defaults_for_batch_rows() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("input.csv");
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());
        Files.writeString(inputFile, """
            datasetId
            doi:10.5072/FK2/ABC
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(mockDatasetVersionResponse());

        var reportsDir = tempDir.resolve("override-reports");
        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString(),
            "--reports-dir", reportsDir.toString(),
            "title=New title"
        );

        assertThat(result.exitCode()).isZero();
        var reportFile = singleReportFileIn(reportsDir);
        assertThat(reportFile.getFileName().toString()).startsWith("input-").endsWith(".csv");
        assertThat(Files.readString(reportFile)).contains("datasetId,result,message")
            .contains("doi:10.5072/FK2/ABC,OK,Metadata edited");
        Mockito.verify(datasetApi).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
    }

    @Test
    void dataset_edit_metadata_uses_default_reports_dir_for_batch_rows() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var reportsDir = tempDir.resolve("default-reports");
        var inputFile = tempDir.resolve("batch-input.csv");
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());
        Files.writeString(inputFile, """
            datasetId
            doi:10.5072/FK2/ABC
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(mockDatasetVersionResponse());

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, reportsDir)),
            "--input-file", inputFile.toString(),
            "title=New title"
        );

        assertThat(result.exitCode()).isZero();
        var reportFile = singleReportFileIn(reportsDir);
        assertThat(reportFile.getFileName().toString()).startsWith("batch-input-").endsWith(".csv");
        assertThat(Files.readString(reportFile)).contains("doi:10.5072/FK2/ABC,OK,Metadata edited");
    }

    @Test
    void dataset_edit_metadata_publishes_when_requested() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(mockDatasetVersionResponse());
        Mockito.when(datasetApi.publish(UpdateType.minor, false)).thenReturn(mockDataMessageResponse());

        var exitCode = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "--publishVersion", "minor",
            "title=New title"
        ).exitCode();

        assertThat(exitCode).isZero();
        Mockito.verify(datasetApi).publish(UpdateType.minor, false);
        Mockito.verify(datasetApi).awaitState("RELEASED", 600000L, 5000L);
    }

    @Test
    void dataset_edit_metadata_publishes_major_when_requested() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(mockDatasetVersionResponse());
        Mockito.when(datasetApi.publish(UpdateType.major, false)).thenReturn(mockDataMessageResponse());

        var exitCode = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "--publishVersion", "major",
            "title=New title"
        ).exitCode();

        assertThat(exitCode).isZero();
        Mockito.verify(datasetApi).publish(UpdateType.major, false);
        Mockito.verify(datasetApi).awaitState("RELEASED", 600000L, 5000L);
    }

    @Test
    void dataset_edit_metadata_leaves_draft_when_requested() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(mockDatasetVersionResponse());

        var exitCode = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "--publishVersion", "leave-draft",
            "title=New title"
        ).exitCode();

        assertThat(exitCode).isZero();
        Mockito.verify(datasetApi).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        Mockito.verify(datasetApi, Mockito.never()).publish(Mockito.any(), Mockito.anyBoolean());
        Mockito.verify(datasetApi, Mockito.never()).awaitState(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());
    }

    @Test
    void dataset_edit_metadata_rejects_reserved_report_columns_in_input() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            datasetId,result
            doi:10.5072/FK2/ABC,ignored
            """);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString(),
            "title=New title"
        );

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stderr()).contains("Input contains reserved column: result");
    }

    @Test
    void dataset_edit_metadata_fails_for_invalid_controlled_vocabulary_value() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "subject=Biology"
        );

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stdout()).contains("FAILED");
        Mockito.verify(datasetApi, Mockito.never()).editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean());
    }

    @Test
    @SuppressWarnings("unchecked")
    void dataverse_metadata_field_spec_provider_loads_and_parses_specs() throws Exception {
        var metadataBlocksApi = Mockito.mock(MetadataBlocksApi.class);
        var response = Mockito.mock(DataverseHttpResponse.class);
        var block = new MetadataBlockDefinition();
        block.setName("citation");

        var title = new DatasetFieldType();
        title.setName("title");
        title.setTypeClass("primitive");
        title.setMultiple(false);

        var subject = new DatasetFieldType();
        subject.setName("subject");
        subject.setTypeClass("controlledVocabulary");
        subject.setMultiple(true);
        subject.setControlledVocabularyValues(List.of("Chemistry", "Computer and Information Science"));

        var authorName = new DatasetFieldType();
        authorName.setName("authorName");
        authorName.setTypeClass("primitive");
        authorName.setMultiple(false);

        var author = new DatasetFieldType();
        author.setName("author");
        author.setTypeClass("compound");
        author.setMultiple(true);
        author.setChildFields(Map.of("authorName", authorName));

        block.setFields(Map.of("title", title, "subject", subject, "author", author));

        Mockito.when(metadataBlocksApi.listMetadataBlocks(false, true)).thenReturn(response);
        Mockito.when(response.getData()).thenReturn(List.of(block));

        var provider = new DatasetEditMetadata.DataverseMetadataFieldSpecProvider(metadataBlocksApi);
        var specs = provider.getFieldSpecs();

        assertThat(specs).containsKeys("title", "subject", "author");
        assertThat(specs.get("title").getTypeClass()).isEqualTo("primitive");
        assertThat(specs.get("title").isMultiple()).isFalse();

        assertThat(specs.get("subject").getTypeClass()).isEqualTo("controlledVocabulary");
        assertThat(specs.get("subject").isMultiple()).isTrue();
        assertThat(specs.get("subject").getControlledVocabularyValues()).containsExactly("Chemistry", "Computer and Information Science");

        assertThat(specs.get("author").getTypeClass()).isEqualTo("compound");
        assertThat(specs.get("author").isMultiple()).isTrue();
        assertThat(specs.get("author").getChildFields()).containsKey("authorName");
        assertThat(specs.get("author").getChildFields().get("authorName").getTypeClass()).isEqualTo("primitive");

        var specs2 = provider.getFieldSpecs();
        assertThat(specs2).isSameAs(specs);
        Mockito.verify(metadataBlocksApi, Mockito.times(1)).listMetadataBlocks(false, true);
    }

    private static Map<String, DatasetFieldType> createFieldSpecs() {
        var title = new DatasetFieldType();
        title.setName("title");
        title.setTypeClass("primitive");
        title.setMultiple(false);

        var subject = new DatasetFieldType();
        subject.setName("subject");
        subject.setTypeClass("controlledVocabulary");
        subject.setMultiple(true);
        subject.setControlledVocabularyValues(List.of("Chemistry", "Medicine, Health & Life Sciences"));

        var authorName = new DatasetFieldType();
        authorName.setName("authorName");
        authorName.setTypeClass("primitive");
        authorName.setMultiple(false);

        var author = new DatasetFieldType();
        author.setName("author");
        author.setTypeClass("compound");
        author.setMultiple(true);
        author.setChildFields(Map.of("authorName", authorName));

        return Map.of(
            "title", title,
            "subject", subject,
            "author", author
        );
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DatasetVersion> mockLatestVersionResponse(String state) throws Exception {
        var version = new DatasetVersion();
        version.setVersionState(state);

        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn(version).when(response).getData();
        return response;
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<List<Lock>> mockLockResponse(List<Lock> locks) throws Exception {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn(locks).when(response).getData();
        return response;
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DatasetVersion> mockDatasetVersionResponse() {
        return Mockito.mock(DataverseHttpResponse.class);
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DataMessage> mockDataMessageResponse() {
        return Mockito.mock(DataverseHttpResponse.class);
    }

    private Path singleReportFileIn(Path reportsDir) throws Exception {
        try (var files = Files.list(reportsDir)) {
            return files.sorted(Comparator.naturalOrder())
                .findFirst()
                .orElseThrow();
        }
    }

    private CapturedExecution executeWithCapturedStdout(CommandLine commandLine, String... args) {
        var originalOut = System.out;
        var originalErr = System.err;
        var out = new ByteArrayOutputStream();
        var err = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(out, true));
            System.setErr(new PrintStream(err, true));
            var exitCode = commandLine.execute(args);
            return new CapturedExecution(exitCode, out.toString(), err.toString());
        }
        finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    private record CapturedExecution(int exitCode, String stdout, String stderr) {
    }
}
