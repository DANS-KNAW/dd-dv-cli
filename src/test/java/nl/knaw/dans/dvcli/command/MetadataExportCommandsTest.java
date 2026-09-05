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

import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.DataverseHttpResponse;
import nl.knaw.dans.lib.dataverse.MetadataExportApi;
import nl.knaw.dans.lib.dataverse.model.DataMessage;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;

public class MetadataExportCommandsTest {

    @Test
    void metadata_export_all_calls_endpoint() throws Exception {
        var api = Mockito.mock(MetadataExportApi.class);
        var response = mockResponse();
        Mockito.when(api.exportAll()).thenReturn(response);

        var exitCode = new CommandLine(new MetadataExportExportAll(api)).execute();

        assertThat(exitCode).isZero();
        Mockito.verify(api).exportAll();
    }

    @Test
    void metadata_re_export_all_passes_filters() throws Exception {
        var api = Mockito.mock(MetadataExportApi.class);
        var response = mockResponse();
        Mockito.when(api.reExportAll("2026-01-01", new String[] { "Datacite", "croissant" })).thenReturn(response);

        var exitCode = new CommandLine(new MetadataExportReExportAll(api)).execute("--older-than", "2026-01-01", "--formats", "Datacite,croissant");

        assertThat(exitCode).isZero();
        Mockito.verify(api).reExportAll("2026-01-01", new String[] { "Datacite", "croissant" });
    }

    @Test
    void metadata_re_export_dataset_uses_pid_when_not_numeric() throws Exception {
        var api = Mockito.mock(MetadataExportApi.class);
        var response = mockResponse();
        Mockito.when(api.reExportDataset("doi:10.5072/FK2/ABC", new String[] { "Datacite" })).thenReturn(response);

        var exitCode = new CommandLine(new MetadataExportReExportDataset(api)).execute("doi:10.5072/FK2/ABC", "--formats", "Datacite");

        assertThat(exitCode).isZero();
        Mockito.verify(api).reExportDataset("doi:10.5072/FK2/ABC", new String[] { "Datacite" });
    }

    @Test
    void metadata_clear_timestamps_returns_non_zero_on_error() throws Exception {
        var api = Mockito.mock(MetadataExportApi.class);
        Mockito.when(api.clearExportTimestamps()).thenThrow(new DataverseException(500, "failure"));

        var exitCode = new CommandLine(new MetadataExportClearExportTimestamps(api)).execute();

        assertThat(exitCode).isEqualTo(1);
        Mockito.verify(api).clearExportTimestamps();
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<Object> mockResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DataMessage> mockDataMessageResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }
}
