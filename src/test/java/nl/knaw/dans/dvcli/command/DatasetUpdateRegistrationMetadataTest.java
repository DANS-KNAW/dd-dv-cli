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
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.DataverseHttpResponse;
import nl.knaw.dans.lib.dataverse.model.DataMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetUpdateRegistrationMetadataTest {
    @TempDir
    Path tempDir;

    @Test
    void dataset_update_registration_metadata_calls_endpoint() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var response = mockResponse();

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.updateRegistrationMetadata()).thenReturn(response);

        var result = executeWithCapturedStdout(new CommandLine(new DatasetUpdateRegistrationMetadata(dataverseClient)), "doi:10.5072/FK2/ABC");

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("{\"status\":\"OK\"}");
        Mockito.verify(datasetApi).updateRegistrationMetadata();
    }

    @Test
    void dataset_update_registration_metadata_returns_non_zero_on_error() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.updateRegistrationMetadata()).thenThrow(new DataverseException(500, "failure"));

        var result = executeWithCapturedStdout(new CommandLine(new DatasetUpdateRegistrationMetadata(dataverseClient)), "doi:10.5072/FK2/ABC");

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stderr()).contains("Error updating registration metadata: failure");
        Mockito.verify(datasetApi).updateRegistrationMetadata();
    }

    @Test
    void dataset_update_registration_metadata_processes_batch_input() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi1 = Mockito.mock(DatasetApi.class);
        var datasetApi2 = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            pid
            doi:10.5072/FK2/ABC
            doi:10.5072/FK2/DEF
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi1);
        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/DEF")).thenReturn(datasetApi2);
        Mockito.when(datasetApi1.updateRegistrationMetadata()).thenReturn(mockResponse());
        Mockito.when(datasetApi2.updateRegistrationMetadata()).thenReturn(mockResponse());

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetUpdateRegistrationMetadata(dataverseClient)),
            "--input-file", inputFile.toString()
        );

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("pid,result,message")
            .contains("doi:10.5072/FK2/ABC,OK,Registration metadata updated")
            .contains("doi:10.5072/FK2/DEF,OK,Registration metadata updated");
        Mockito.verify(datasetApi1).updateRegistrationMetadata();
        Mockito.verify(datasetApi2).updateRegistrationMetadata();
    }

    @Test
    void dataset_update_registration_metadata_returns_non_zero_when_batch_row_fails() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            pid
            doi:10.5072/FK2/ABC
            """);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.updateRegistrationMetadata()).thenThrow(new DataverseException(500, "failure"));

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetUpdateRegistrationMetadata(dataverseClient)),
            "--input-file", inputFile.toString()
        );

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stdout()).contains("pid,result,message")
            .contains("doi:10.5072/FK2/ABC,FAILED,failure");
        Mockito.verify(datasetApi).updateRegistrationMetadata();
    }

    @Test
    void dataset_update_registration_metadata_rejects_pid_with_input_file() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var inputFile = tempDir.resolve("input.csv");
        Files.writeString(inputFile, """
            pid
            doi:10.5072/FK2/ABC
            """);

        var result = executeWithCapturedStdout(
            new CommandLine(new DatasetUpdateRegistrationMetadata(dataverseClient)),
            "--input-file", inputFile.toString(),
            "doi:10.5072/FK2/XYZ"
        );

        assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.stderr()).contains("PID cannot be used together with --input-file");
        Mockito.verifyNoInteractions(dataverseClient);
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DataMessage> mockResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
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
