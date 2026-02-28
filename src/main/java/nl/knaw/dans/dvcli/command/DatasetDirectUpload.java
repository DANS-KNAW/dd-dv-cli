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

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.lib.dataverse.DataverseClient;
import nl.knaw.dans.lib.dataverse.DataverseException;
import nl.knaw.dans.lib.dataverse.model.dataset.DirectUploadURLs;
import nl.knaw.dans.lib.dataverse.model.file.prestaged.Checksum;
import nl.knaw.dans.lib.dataverse.model.file.prestaged.PrestagedFile;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.FileEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

@Command(name = "dataset-direct-upload",
         description = "Uploads a file directly to S3 and registers it in the dataset",
         mixinStandardHelpOptions = true)
@Slf4j
public class DatasetDirectUpload extends AbstractDatasetCmd implements Callable<Integer> {
    private final URI baseUrl;
    private final String apiToken;

    @Parameters(index = "1", paramLabel = "FILE", description = "Path to the file to upload")
    private Path file;

    @Option(names = { "--directory-label" }, description = "Directory label for the file in the dataset")
    private String directoryLabel;

    @Option(names = { "--description" }, description = "Description for the file")
    private String description = "File uploaded via dd-dv-cli";

    public DatasetDirectUpload(DataverseClient dataverseClient, URI baseUrl, String apiToken) {
        super(dataverseClient);
        this.baseUrl = baseUrl;
        this.apiToken = apiToken;
    }

    @Override
    public Integer call() throws Exception {
        if (!Files.exists(file)) {
            System.err.println("File not found: " + file);
            return 1;
        }

        long fileSize = Files.size(file);
        String sha1Checksum;
        try (InputStream is = Files.newInputStream(file)) {
            sha1Checksum = DigestUtils.sha1Hex(is);
        }

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            log.info("Requesting upload URLs for file size: {}", fileSize);
            System.err.print("Requesting upload URLs for file size: " + fileSize + "...");
            DirectUploadURLs uploadUrls = getDatasetApi().getUploadUrls(fileSize).getData();
            System.err.println("OK");
            if (uploadUrls.getUrl() != null) {
                System.err.println("Single part upload");
                log.info("Single part upload");
                uploadSinglePart(httpClient, uploadUrls.getUrl(), file);
            }
            else if (uploadUrls.getUrls() != null) {
                System.err.println("Multi part upload");
                log.info("Multi part upload");
                uploadMultiPart(httpClient, uploadUrls, file);
            }
            else {
                throw new IllegalStateException("No upload URL(s) provided by Dataverse");
            }

            log.info("Registering file in Dataverse");
            System.err.print("Registering file in Dataverse...");
            PrestagedFile prestagedFile = new PrestagedFile();
            prestagedFile.setStorageIdentifier(uploadUrls.getStorageIdentifier());
            prestagedFile.setFileName(file.getFileName().toString());
            prestagedFile.setMimeType(Files.probeContentType(file));
            if (prestagedFile.getMimeType() == null) {
                prestagedFile.setMimeType("application/octet-stream");
            }
            prestagedFile.setChecksum(new Checksum("SHA-1", sha1Checksum));
            prestagedFile.setDescription(description);
            prestagedFile.setDirectoryLabel(directoryLabel);

            var response = getDatasetApi().addFile(prestagedFile);
            System.err.println("OK");
            log.debug("Response: {}", response.getEnvelopeAsString());

            return 0;
        }
        catch (DataverseException e) {
            System.err.println("Error interacting with Dataverse: " + e.getMessage());
            return 1;
        }
        catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
            return 1;
        }
    }

    private void uploadSinglePart(CloseableHttpClient httpClient, String url, Path file) throws IOException {
        log.info("Performing single-part upload to S3");
        HttpPut putRequest = new HttpPut(url);
        putRequest.setEntity(new FileEntity(file.toFile(), ContentType.APPLICATION_OCTET_STREAM));
        putRequest.setHeader("x-amz-tagging", "dv-state=temp");

        httpClient.execute(putRequest, response -> {
            if (response.getCode() >= 300) {
                throw new IOException("Failed to upload file to S3: " + response.getReasonPhrase());
            }
            return null;
        });
    }

    private void uploadMultiPart(CloseableHttpClient httpClient, DirectUploadURLs uploadUrls, Path file) throws IOException {
        log.info("Performing multi-part upload to S3");
        Map<String, String> etags = new HashMap<>();
        long partSize = uploadUrls.getPartSize();
        Map<String, String> partUrls = uploadUrls.getUrls();

        long fileSize = Files.size(file);
        for (Map.Entry<String, String> entry : partUrls.entrySet()) {
            String partNumber = entry.getKey();
            String url = entry.getValue();

            log.debug("Uploading part {} to {}", partNumber, url);
            System.err.print("Uploading part " + partNumber + " of " + partUrls.size() + "...");
            HttpPut putRequest = new HttpPut(url);

            long offset = (Long.parseLong(partNumber) - 1) * partSize;
            long remaining = fileSize - offset;
            long currentPartSize = Math.min(partSize, remaining);

            try (InputStream is = new BufferedInputStream(new FileInputStream(file.toFile()))) {
                if (offset > 0) {
                    long skipped = is.skip(offset);
                    if (skipped != offset) {
                        throw new IOException("Failed to skip to offset " + offset + " for part " + partNumber);
                    }
                }
                putRequest.setEntity(new InputStreamEntity(is, currentPartSize, ContentType.APPLICATION_OCTET_STREAM));

                String etag = httpClient.execute(putRequest, response -> {
                    if (response.getCode() >= 300) {
                        throw new IOException("Failed to upload part " + partNumber + " to S3: " + response.getReasonPhrase());
                    }
                    return response.getFirstHeader("ETag").getValue();
                });
                etags.put(partNumber, etag);
            }
            System.err.println("OK");
        }

        log.info("Completing multi-part upload");
        System.err.print("Completing multi-part upload...");
        String completeUrl = baseUrl.toString() + (baseUrl.toString().endsWith("/") ? "" : "/") + uploadUrls.getComplete();
        HttpPut completeRequest = new HttpPut(completeUrl);

        completeRequest.setHeader("X-Dataverse-key", apiToken);
        var objectMapper = new ObjectMapper();
        completeRequest.setEntity(new StringEntity(objectMapper.writeValueAsString(etags), ContentType.APPLICATION_JSON));

        httpClient.execute(completeRequest, response -> {
            if (response.getCode() >= 300) {
                throw new IOException("Failed to complete multi-part upload: " + response.getReasonPhrase());
            }
            System.err.println("OK");
            return null;
        });

        System.err.println("File registered successfully.");
    }
}
