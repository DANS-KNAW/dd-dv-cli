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
import nl.knaw.dans.lib.dataverse.model.dataset.MetadataBlock;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DatasetEditMetadataTest {
    @TempDir
    Path tempDir;

    @Test
    void explicit_unrestricted_expectations_are_rejected() {
        assertThatThrownBy(() -> DatasetEditMetadata.ExpectedState.parse("any"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid expectedState: any");
        assertThatThrownBy(() -> DatasetEditMetadata.ReviewExpectation.parse("either"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid expectInReview: either");
    }

    @Test
    void publishVersion_no_is_synonym_for_leave_draft() {
        assertThat(DatasetEditMetadata.PublishVersion.parse("no")).isEqualTo(DatasetEditMetadata.PublishVersion.LEAVE_DRAFT);
    }

    @Test
    void timeout_accepts_units_and_defaults_to_seconds() {
        var converter = new TimeoutConverter();

        assertThat(converter.convert("10s").toSeconds()).isEqualTo(10);
        assertThat(converter.convert("2m").toSeconds()).isEqualTo(120);
        assertThat(converter.convert("30").toSeconds()).isEqualTo(30);
    }

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
            "TITLE=New title",
            "subject=Chemistry",
            "AUTHOR.AUTHORNAME=Alice"
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
    void dataset_edit_metadata_accepts_any_state_and_review_status_when_unspecified() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var latestVersionResponse = mockLatestVersionResponse("DRAFT");
        var lock = new Lock();
        lock.setLockType("InReview");
        var lockResponse = mockLockResponse(List.of(lock));

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "title=New title"
        );

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("OK");
        Mockito.verify(datasetApi).editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean());
    }

    @Test
    void dataset_edit_metadata_skips_when_explicit_expected_state_does_not_match() throws Exception {
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
            "--expectedState", "released",
            "title=New title"
        );

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("SKIPPED");
        Mockito.verify(datasetApi, Mockito.never()).editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean());
    }

    @Test
    void dataset_edit_metadata_reads_field_values_from_input_file() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("input.csv");
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());
        Files.writeString(inputFile, """
            *DATASETID,TiTlE
            doi:10.5072/FK2/ABC,CSV title
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(mockDatasetVersionResponse());

        var reportsDir = tempDir.resolve("override-reports");
        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString(),
            "--reports-dir", reportsDir.toString()
        );

        assertThat(result.exitCode()).isZero();
        var reportFile = singleReportFileIn(reportsDir);
        assertThat(reportFile.getFileName().toString()).startsWith("input-").endsWith(".csv");
        assertThat(Files.readString(reportFile)).contains("*DATASETID,*result,*message,*expectedState,*replace,TiTlE")
            .contains("doi:10.5072/FK2/ABC,OK,Metadata edited,not deaccessioned,false,CSV title");
        Mockito.verify(datasetApi).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
    }

    @Test
    void dataset_edit_metadata_accepts_special_columns_without_star() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("unstarred.csv");
        Files.writeString(inputFile, """
            pid,title
            doi:10.5072/FK2/ABC,CSV title
            """);
        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());
        var editResponse = mockDatasetVersionResponse();
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(editResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("pid,*result,*message,*expectedState,*replace,title").contains("doi:10.5072/FK2/ABC,OK,Metadata edited,not deaccessioned,false,CSV title");
    }

    @Test
    void dataset_edit_metadata_treats_unstarred_column_as_field_when_starred_column_is_present() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("conflict.csv");
        Files.writeString(inputFile, """
            *pid,pid
            doi:10.5072/FK2/ABC,field value
            """);
        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());
        var editResponse = mockDatasetVersionResponse();
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(editResponse);
        var pidField = new DatasetFieldType();
        pidField.setName("pid");
        pidField.setTypeClass("primitive");
        pidField.setMultiple(false);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, () -> Map.of("pid", pidField), null)),
            "--input-file", inputFile.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("doi:10.5072/FK2/ABC,OK,Metadata edited,not deaccessioned,false,field value");
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
            *datasetId,title
            doi:10.5072/FK2/ABC,New title
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(mockDatasetVersionResponse());

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, reportsDir)),
            "--input-file", inputFile.toString()
        );

        assertThat(result.exitCode()).isZero();
        var reportFile = singleReportFileIn(reportsDir);
        assertThat(reportFile.getFileName().toString()).startsWith("batch-input-").endsWith(".csv");
        assertThat(Files.readString(reportFile)).contains("doi:10.5072/FK2/ABC,OK,Metadata edited,not deaccessioned,false,New title");
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
        Mockito.verify(datasetApi).awaitState("RELEASED", 10000L, 5000L);
    }

    @Test
    void dataset_edit_metadata_sets_version_note_before_publishing() throws Exception {
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
            "--versionNote", "Fixed title",
            "title=New title"
        ).exitCode();

        assertThat(exitCode).isZero();
        var inOrder = Mockito.inOrder(datasetApi);
        inOrder.verify(datasetApi).setVersionNote("Fixed title");
        inOrder.verify(datasetApi).publish(UpdateType.minor, false);
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
        Mockito.verify(datasetApi).awaitState("RELEASED", 10000L, 5000L);
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
    void dataset_edit_metadata_uses_cli_defaults_for_blank_instruction_columns() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("defaults.csv");
        Files.writeString(inputFile, """
            *datasetId,*expectedState,*publishVersion,title
            doi:10.5072/FK2/ABC,,,Default title
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        var locksResponse = mockLockResponse(List.of());
        Mockito.when(datasetApi.getLocks()).thenReturn(locksResponse);
        var editResponse = mockDatasetVersionResponse();
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(editResponse);
        var publishResponse = mockDataMessageResponse();
        Mockito.when(datasetApi.publish(UpdateType.minor, false)).thenReturn(publishResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString(),
            "--expectedState", "released",
            "--publishVersion", "minor"
        );

        assertThat(result.exitCode()).isZero();
        var fields = ArgumentCaptor.forClass(FieldList.class);
        Mockito.verify(datasetApi).editMetadata(fields.capture(), Mockito.eq(false));
        assertThat(((PrimitiveSingleValueField) fields.getValue().getFields().get(0)).getValue()).isEqualTo("Default title");
        Mockito.verify(datasetApi).publish(UpdateType.minor, false);
        Mockito.verify(datasetApi).awaitState("RELEASED", 10000L, 5000L);
    }

    @Test
    void dataset_edit_metadata_clears_a_field_for_an_empty_cell_when_replacing() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            *datasetId,subject
            doi:10.5072/FK2/ABC,
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        var locksResponse = mockLockResponse(List.of());
        Mockito.when(datasetApi.getLocks()).thenReturn(locksResponse);
        var editResponse = mockDatasetVersionResponse();
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean())).thenReturn(editResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString(), "--replace"
        );

        assertThat(result.exitCode()).isZero();
        var captor = ArgumentCaptor.forClass(FieldList.class);
        Mockito.verify(datasetApi).editMetadata(captor.capture(), Mockito.eq(true));
        assertThat(captor.getValue().getFields()).hasSize(1);
        assertThat(((ControlledMultiValueField) captor.getValue().getFields().get(0)).getValue()).isEmpty();
    }

    @Test
    void dataset_edit_metadata_ignores_an_empty_cell_when_not_replacing() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            *datasetId,title,subject
            doi:10.5072/FK2/ABC,New title,
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        var locksResponse = mockLockResponse(List.of());
        Mockito.when(datasetApi.getLocks()).thenReturn(locksResponse);
        var editResponse = mockDatasetVersionResponse();
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean())).thenReturn(editResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString()
        );

        assertThat(result.exitCode()).isZero();
        var captor = ArgumentCaptor.forClass(FieldList.class);
        Mockito.verify(datasetApi).editMetadata(captor.capture(), Mockito.eq(false));
        assertThat(captor.getValue().getFields()).hasSize(1);
    }

    @Test
    void dataset_edit_metadata_skips_cells_with_the_skip_code_when_replacing() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            *datasetId,title,subject
            doi:10.5072/FK2/ABC,New title,!skip
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        var locksResponse = mockLockResponse(List.of());
        Mockito.when(datasetApi.getLocks()).thenReturn(locksResponse);
        var editResponse = mockDatasetVersionResponse();
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean())).thenReturn(editResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString(), "--replace"
        );

        assertThat(result.exitCode()).isZero();
        var captor = ArgumentCaptor.forClass(FieldList.class);
        Mockito.verify(datasetApi).editMetadata(captor.capture(), Mockito.eq(true));
        assertThat(captor.getValue().getFields()).hasSize(1);
        assertThat(((PrimitiveSingleValueField) captor.getValue().getFields().get(0)).getValue()).isEqualTo("New title");
    }

    @Test
    void field_value_accumulator_rejects_mixing_skip_and_values_in_either_order() {
        var subject = createFieldSpecs().get("subject");

        var valueThenSkip = new FieldValueAccumulator(subject);
        valueThenSkip.addValue(1, "Chemistry", "subject");
        assertThatThrownBy(valueThenSkip::markSkipped).isInstanceOf(IllegalArgumentException.class);

        var skipThenValue = new FieldValueAccumulator(subject);
        skipThenValue.markSkipped();
        assertThatThrownBy(() -> skipThenValue.addValue(2, "Chemistry", "subject[2]")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void dataset_edit_metadata_uses_a_custom_skip_code() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            *datasetId,title,subject
            doi:10.5072/FK2/ABC,!skip,SKIP
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        var locksResponse = mockLockResponse(List.of());
        Mockito.when(datasetApi.getLocks()).thenReturn(locksResponse);
        var editResponse = mockDatasetVersionResponse();
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean())).thenReturn(editResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString(), "--replace", "--skip-code", "SKIP"
        );

        assertThat(result.exitCode()).isZero();
        var captor = ArgumentCaptor.forClass(FieldList.class);
        Mockito.verify(datasetApi).editMetadata(captor.capture(), Mockito.eq(true));
        assertThat(captor.getValue().getFields()).hasSize(1);
        assertThat(((PrimitiveSingleValueField) captor.getValue().getFields().get(0)).getValue()).isEqualTo("!skip");
    }

    @Test
    void dataset_edit_metadata_rejects_reserved_report_columns_in_input() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            *datasetId,*result
            doi:10.5072/FK2/ABC,ignored
            """);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString()
        );

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stderr()).contains("Input contains reserved column: *result");
    }

    @Test
    void dataset_edit_metadata_accepts_pid_and_database_id_options() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(dataverseClient.dataset(42)).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(mockDatasetVersionResponse());

        assertThat(executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--pid", "doi:10.5072/FK2/ABC", "title=New title").exitCode()).isZero();
        assertThat(executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--databaseId", "42", "title=New title").exitCode()).isZero();
        Mockito.verify(dataverseClient).dataset("doi:10.5072/FK2/ABC");
        Mockito.verify(dataverseClient).dataset(42);
    }

    @Test
    void dataset_edit_metadata_rejects_multiple_identifier_options() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--pid", "doi:10.5072/FK2/ABC", "--databaseId", "42", "title=New title");

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stderr()).contains("mutually exclusive");
    }

    @Test
    void dataset_edit_metadata_validates_identifier_formats_per_row() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var pidFile = tempDir.resolve("pids.csv");
        Files.writeString(pidFile, """
            *pid,title
            10.5072/FK2/ABC,T
            """);
        var databaseIdFile = tempDir.resolve("database-ids.csv");
        Files.writeString(databaseIdFile, """
            *databaseId,title
            12a,T
            """);

        var pidResult = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", pidFile.toString());
        var databaseIdResult = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", databaseIdFile.toString());

        assertThat(pidResult.exitCode()).isEqualTo(1);
        assertThat(pidResult.stdout()).contains("Invalid pid");
        assertThat(databaseIdResult.exitCode()).isEqualTo(1);
        assertThat(databaseIdResult.stdout()).contains("Invalid databaseId");
        Mockito.verifyNoInteractions(dataverseClient);
    }

    @Test
    void dataset_edit_metadata_requires_exactly_one_identifier_column_in_input_file() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var twoColumns = tempDir.resolve("two.csv");
        Files.writeString(twoColumns, """
            *pid,*DatabaseId,title
            doi:10.5072/FK2/ABC,42,T
            """);
        var noColumns = tempDir.resolve("none.csv");
        Files.writeString(noColumns, """
            title
            T
            """);

        var twoResult = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", twoColumns.toString());
        var noneResult = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", noColumns.toString());

        assertThat(twoResult.exitCode()).isEqualTo(1);
        assertThat(twoResult.stderr()).contains("only one of the columns");
        assertThat(noneResult.exitCode()).isEqualTo(1);
        assertThat(noneResult.stderr()).contains("must contain one of the columns");
        Mockito.verifyNoInteractions(dataverseClient);
    }

    @Test
    void dataset_edit_metadata_rejects_identifier_options_together_with_input_file() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            title
            T
            """);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString(),
            "--pid", "doi:10.5072/FK2/ABC"
        );

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stderr()).contains("cannot be used together with --input-file");
        Mockito.verifyNoInteractions(dataverseClient);
    }

    @Test
    void dataset_edit_metadata_applies_field_defaults_with_input_file_and_reports_them() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("defaults.csv");
        Files.writeString(inputFile, """
            *pid
            doi:10.5072/FK2/ABC
            """);
        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        var latestVersionResponse = mockLatestVersionResponse("RELEASED");
        var lockResponse = mockLockResponse(List.of());
        var editResponse = mockDatasetVersionResponse();
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(editResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", inputFile.toString(),
            "title=Default title"
        );

        assertThat(result.exitCode()).isZero();
        Mockito.verify(datasetApi).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        assertThat(result.stdout()).contains("*pid,*result,*message,*expectedState,*replace,title").contains("doi:10.5072/FK2/ABC,OK,Metadata edited,not deaccessioned,false,Default title");
    }

    @Test
    void dataset_edit_metadata_skips_publication_for_draft_when_expected_state_is_not_explicit() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        var latestVersionResponse = mockLatestVersionResponse("DRAFT");
        var lockResponse = mockLockResponse(List.of());
        var editResponse = mockDatasetVersionResponse();
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(latestVersionResponse);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);
        Mockito.when(datasetApi.editMetadata(Mockito.any(FieldList.class), Mockito.eq(false))).thenReturn(editResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--pid", "doi:10.5072/FK2/ABC",
            "--publishVersion", "minor",
            "title=T"
        );

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("not deaccessioned").contains("Metadata edited, publication skipped (existing draft)");
        Mockito.verify(datasetApi).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        Mockito.verify(datasetApi, Mockito.never()).publish(Mockito.any(), Mockito.anyBoolean());
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

    @Test
    void expected_value_mismatch_skips_row() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var response = mockLatestVersionResponse("RELEASED");
        setTitle(response.getData(), "Actual title");
        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(response);
        var lockResponse = mockLockResponse(List.of());
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "=title=Other title",
            "title=New title"
        );

        assertThat(result.stdout()).contains("SKIPPED").contains("Actual title");
        Mockito.verify(datasetApi, Mockito.never()).editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean());
    }

    @Test
    void expected_value_match_allows_edit() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var response = mockLatestVersionResponse("RELEASED");
        setTitle(response.getData(), "Actual title");
        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(response);
        var lockResponse = mockLockResponse(List.of());
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "=title=Actual title",
            "title=New title"
        );

        assertThat(result.exitCode()).isZero();
        Mockito.verify(datasetApi).editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean());
    }

    @Test
    void empty_expected_value_requires_empty_field() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var response = mockLatestVersionResponse("RELEASED");
        setTitle(response.getData(), "Actual title");
        var lockResponse = mockLockResponse(List.of());
        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.getVersion(Version.LATEST.toString())).thenReturn(response);
        Mockito.when(datasetApi.getLocks()).thenReturn(lockResponse);

        var skipped = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "=title=",
            "title=New title"
        );
        assertThat(skipped.stdout()).contains("SKIPPED");

        var ok = executeWithCapturedStdout(
            new CommandLine(new DatasetEditMetadata(dataverseClient, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--datasetId", "doi:10.5072/FK2/ABC",
            "=subject=",
            "title=New title"
        );
        assertThat(ok.exitCode()).isZero();
        Mockito.verify(datasetApi).editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean());
    }

    @Test
    void failures_after_starting_state_is_captured_retain_it_for_the_next_edit() throws Exception {
        for (var failure : List.of("edit", "note", "publish", "poll")) {
            var client = Mockito.mock(DataverseClient.class);
            var api = Mockito.mock(DatasetApi.class);
            var input = tempDir.resolve(failure + ".csv");
            Files.writeString(input, """
                *datasetId,*publishVersion,*versionNote,title
                42,minor,First note,First
                42,minor,,Second
                """);
            Mockito.when(client.dataset(42)).thenReturn(api);
            Mockito.doReturn(mockLatestVersionResponse("RELEASED"), mockLatestVersionResponse("DRAFT")).when(api).getVersion(Version.LATEST.toString());
            Mockito.doReturn(mockLockResponse(List.of())).when(api).getLocks();
            switch (failure) {
                case "edit" -> Mockito.doThrow(new RuntimeException("edit failure")).doReturn(null)
                    .when(api).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
                case "note" -> Mockito.doThrow(new RuntimeException("note failure")).when(api).setVersionNote("First note");
                case "publish" -> Mockito.doThrow(new RuntimeException("publish failure")).doReturn(null)
                    .when(api).publish(UpdateType.minor, false);
                case "poll" -> Mockito.doThrow(new RuntimeException("poll failure")).doNothing()
                    .when(api).awaitState("RELEASED", 10000L, 5000L);
            }

            var result = executeWithCapturedStdout(new CommandLine(new DatasetEditMetadata(client, DatasetEditMetadataTest::createFieldSpecs, null)),
                "--input-file", input.toString());

            assertThat(result.exitCode()).as(failure).isEqualTo(1);
            assertThat(result.stdout()).as(failure).contains("FAILED").contains("42,OK").doesNotContain("publication skipped (existing draft)");
            Mockito.verify(api, Mockito.times(failure.equals("publish") || failure.equals("poll") ? 2 : 1)).publish(UpdateType.minor, false);
        }
    }

    @Test
    void same_dataset_skips_preserve_captured_starting_state() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        var input = tempDir.resolve("same-skips.csv");
        Files.writeString(input, """
            *datasetId,*expectedState,*expectInReview,*publishVersion,=title,title
            42,,,leave-draft,,First
            42,released,,leave-draft,,State skip
            42,,yes,leave-draft,,Review skip
            42,,,leave-draft,Wrong,Value skip
            42,,,minor,,Last
            """);
        Mockito.when(client.dataset(42)).thenReturn(api);
        Mockito.doReturn(mockLatestVersionResponse("RELEASED"), mockLatestVersionResponse("DRAFT"))
            .when(api).getVersion(Version.LATEST.toString());
        Mockito.doReturn(mockLockResponse(List.of())).when(api).getLocks();

        var result = executeWithCapturedStdout(new CommandLine(new DatasetEditMetadata(client, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", input.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("Expected state", "Expected in review", "Expected title to be 'Wrong'", "42,OK")
            .doesNotContain("publication skipped (existing draft)");
        Mockito.verify(api, Mockito.times(2)).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        Mockito.verify(api).publish(UpdateType.minor, false);
    }

    @Test
    void confirmed_publication_resets_starting_state_for_next_edit() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        var input = tempDir.resolve("published.csv");
        Files.writeString(input, """
            *datasetId,*publishVersion,title
            42,minor,First
            42,minor,Second
            """);
        Mockito.when(client.dataset(42)).thenReturn(api);
        Mockito.doReturn(mockLatestVersionResponse("RELEASED"), mockLatestVersionResponse("DRAFT")).when(api).getVersion(Version.LATEST.toString());
        Mockito.doReturn(mockLockResponse(List.of())).when(api).getLocks();

        var result = executeWithCapturedStdout(new CommandLine(new DatasetEditMetadata(client, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", input.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("publication skipped (existing draft)");
        Mockito.verify(api, Mockito.times(2)).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        Mockito.verify(api).publish(UpdateType.minor, false);
        Mockito.verify(api).awaitState("RELEASED", 10000L, 5000L);
    }

    @Test
    void starting_state_is_isolated_between_invocations_of_same_command() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        var input = tempDir.resolve("invocations.csv");
        Files.writeString(input, "*datasetId,*publishVersion,title\n42,leave-draft,Edit\n");
        Mockito.when(client.dataset(42)).thenReturn(api);
        Mockito.doReturn(mockLatestVersionResponse("RELEASED"), mockLatestVersionResponse("DRAFT")).when(api).getVersion(Version.LATEST.toString());
        Mockito.doReturn(mockLockResponse(List.of())).when(api).getLocks();
        var command = new CommandLine(new DatasetEditMetadata(client, DatasetEditMetadataTest::createFieldSpecs, null));

        var first = executeWithCapturedStdout(command, "--input-file", input.toString());
        Files.writeString(input, "*datasetId,*publishVersion,title\n42,minor,Edit\n");
        var second = executeWithCapturedStdout(command, "--input-file", input.toString());

        assertThat(first.exitCode()).isZero();
        assertThat(second.exitCode()).isZero();
        assertThat(second.stdout()).contains("publication skipped (existing draft)");
        Mockito.verify(api, Mockito.times(2)).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        Mockito.verify(api, Mockito.never()).publish(Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    void consecutive_aliases_share_the_starting_released_state() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        var input = tempDir.resolve("aliases.csv");
        Files.writeString(input, """
            *datasetId,*publishVersion,title
            doi:10.5072/FK2/ABC,leave-draft,First
            42,minor,Second
            """);
        Mockito.when(client.dataset("doi:10.5072/FK2/ABC")).thenReturn(api);
        Mockito.when(client.dataset(42)).thenReturn(api);
        Mockito.doReturn(mockLatestVersionResponse("RELEASED"), mockLatestVersionResponse("DRAFT")).when(api).getVersion(Version.LATEST.toString());
        Mockito.doReturn(mockLockResponse(List.of())).when(api).getLocks();

        var result = executeWithCapturedStdout(new CommandLine(new DatasetEditMetadata(client, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", input.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("doi:10.5072/FK2/ABC,OK").contains("42,OK");
        var fields = ArgumentCaptor.forClass(FieldList.class);
        Mockito.verify(api, Mockito.times(2)).editMetadata(fields.capture(), Mockito.eq(false));
        assertThat(fields.getAllValues()).extracting(f -> ((PrimitiveSingleValueField) f.getFields().get(0)).getValue())
            .containsExactly("First", "Second");
        Mockito.verify(api).publish(UpdateType.minor, false);
    }

    @Test
    void interleaved_dataset_starts_a_new_group_and_explicit_expectation_allows_publication() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var first = Mockito.mock(DatasetApi.class);
        var second = Mockito.mock(DatasetApi.class);
        var input = tempDir.resolve("interleaved.csv");
        Files.writeString(input, """
            *datasetId,*expectedState,*publishVersion,title
            42,,leave-draft,First
            43,,leave-draft,Other
            42,,minor,Return
            42,draft,minor,Explicit
            """);
        Mockito.when(client.dataset(42)).thenReturn(first);
        Mockito.when(client.dataset(43)).thenReturn(second);
        Mockito.doReturn(mockLatestVersionResponse("RELEASED", 42), mockLatestVersionResponse("DRAFT", 42),
            mockLatestVersionResponse("DRAFT", 42)).when(first).getVersion(Version.LATEST.toString());
        Mockito.doReturn(mockLatestVersionResponse("RELEASED", 43)).when(second).getVersion(Version.LATEST.toString());
        Mockito.doReturn(mockLockResponse(List.of())).when(first).getLocks();
        Mockito.doReturn(mockLockResponse(List.of())).when(second).getLocks();

        var result = executeWithCapturedStdout(new CommandLine(new DatasetEditMetadata(client, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", input.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("publication skipped (existing draft)");
        Mockito.verify(first, Mockito.times(3)).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        Mockito.verify(second).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        Mockito.verify(first).publish(UpdateType.minor, false);
    }

    @Test
    void skipped_row_does_not_capture_starting_state_before_first_edit() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        var input = tempDir.resolve("skipped.csv");
        Files.writeString(input, """
            *datasetId,*expectedState,*publishVersion,title
            42,draft,leave-draft,Skipped
            42,,minor,Edited
            """);
        Mockito.when(client.dataset(42)).thenReturn(api);
        Mockito.doReturn(mockLatestVersionResponse("RELEASED"), mockLatestVersionResponse("DRAFT")).when(api).getVersion(Version.LATEST.toString());
        Mockito.doReturn(mockLockResponse(List.of())).when(api).getLocks();

        var result = executeWithCapturedStdout(new CommandLine(new DatasetEditMetadata(client, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", input.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("SKIPPED").contains("publication skipped (existing draft)");
        Mockito.verify(api).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        Mockito.verify(api, Mockito.never()).publish(Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    void invalid_and_unresolved_rows_break_consecutive_groups() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        var unresolved = Mockito.mock(DatasetApi.class);
        var input = tempDir.resolve("interruptions.csv");
        Files.writeString(input, """
            *datasetId,*publishVersion,title
            42,leave-draft,First
            ,minor,Missing
            42,minor,After missing
            unknown,minor,Unresolved
            42,minor,After unresolved
            """);
        Mockito.when(client.dataset(42)).thenReturn(api);
        Mockito.when(client.dataset("unknown")).thenReturn(unresolved);
        Mockito.doReturn(mockLatestVersionResponse("RELEASED"), mockLatestVersionResponse("DRAFT"),
            mockLatestVersionResponse("DRAFT")).when(api).getVersion(Version.LATEST.toString());
        Mockito.when(unresolved.getVersion(Version.LATEST.toString())).thenThrow(new IllegalStateException("not found"));
        Mockito.doReturn(mockLockResponse(List.of())).when(api).getLocks();

        var result = executeWithCapturedStdout(new CommandLine(new DatasetEditMetadata(client, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--input-file", input.toString());

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stdout()).contains("Missing dataset identifier", "not found", "publication skipped (existing draft)");
        Mockito.verify(api, Mockito.times(3)).editMetadata(Mockito.any(FieldList.class), Mockito.eq(false));
        Mockito.verify(api, Mockito.never()).publish(Mockito.any(), Mockito.anyBoolean());
    }

    @Test
    void missing_canonical_id_fails_row_without_editing() throws Exception {
        var client = Mockito.mock(DataverseClient.class);
        var api = Mockito.mock(DatasetApi.class);
        Mockito.when(client.dataset(42)).thenReturn(api);
        Mockito.doReturn(mockLatestVersionResponse("RELEASED", null)).when(api).getVersion(Version.LATEST.toString());

        var result = executeWithCapturedStdout(new CommandLine(new DatasetEditMetadata(client, DatasetEditMetadataTest::createFieldSpecs, null)),
            "--databaseId", "42", "title=New title");

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stdout()).contains("Latest version is missing canonical dataset ID");
        Mockito.verify(api, Mockito.never()).editMetadata(Mockito.any(FieldList.class), Mockito.anyBoolean());
    }

    private static void setTitle(DatasetVersion version, String title) {
        var block = new MetadataBlock();
        block.setFields(List.of(new PrimitiveSingleValueField("title", title)));
        version.setMetadataBlocks(Map.of("citation", block));
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
        return mockLatestVersionResponse(state, 42);
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DatasetVersion> mockLatestVersionResponse(String state, Integer datasetId) throws Exception {
        var version = new DatasetVersion();
        version.setVersionState(state);
        version.setDatasetId(datasetId);

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
