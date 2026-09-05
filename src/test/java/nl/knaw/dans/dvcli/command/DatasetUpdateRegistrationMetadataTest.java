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
import org.mockito.Mockito;
import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;

public class DatasetUpdateRegistrationMetadataTest {

    @Test
    void dataset_update_registration_metadata_calls_endpoint() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);
        var response = mockResponse();

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.updateRegistrationMetadata()).thenReturn(response);

        var exitCode = new CommandLine(new DatasetUpdateRegistrationMetadata(dataverseClient)).execute("doi:10.5072/FK2/ABC");

        assertThat(exitCode).isZero();
        Mockito.verify(datasetApi).updateRegistrationMetadata();
    }

    @Test
    void dataset_update_registration_metadata_returns_non_zero_on_error() throws Exception {
        var dataverseClient = Mockito.mock(DataverseClient.class);
        var datasetApi = Mockito.mock(DatasetApi.class);

        Mockito.when(dataverseClient.dataset("doi:10.5072/FK2/ABC")).thenReturn(datasetApi);
        Mockito.when(datasetApi.updateRegistrationMetadata()).thenThrow(new DataverseException(500, "failure"));

        var exitCode = new CommandLine(new DatasetUpdateRegistrationMetadata(dataverseClient)).execute("doi:10.5072/FK2/ABC");

        assertThat(exitCode).isEqualTo(1);
        Mockito.verify(datasetApi).updateRegistrationMetadata();
    }

    @SuppressWarnings("unchecked")
    private static DataverseHttpResponse<DataMessage> mockResponse() {
        var response = Mockito.mock(DataverseHttpResponse.class);
        Mockito.doReturn("{\"status\":\"OK\"}").when(response).getEnvelopeAsString();
        return response;
    }
}
