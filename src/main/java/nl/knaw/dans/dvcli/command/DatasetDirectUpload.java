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
import nl.knaw.dans.dvcli.model.DirectUploadState;
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

    @Option(names = { "--label" }, description = "Label for the file in the dataset (defaults to the file name)")
    private String label;

    @Option(names = { "--directory-label", "-d" }, description = "Directory label for the file in the dataset")
    private String directoryLabel;

    @Option(names = { "--description" }, description = "Description for the file", defaultValue = "")
    private String description;

    @Option(names = { "--resume" }, description = "Resume the upload from the upload-state file")
    private boolean resume;

    @Option(names = { "--keep-upload-state" }, description = "Prevent the upload-state file from being automatically deleted. Note that the upload-state "
        + "file is always created for a multi-part upload; this option only controls whether it is deleted after a successful upload.")
    private boolean keepUploadState;

    private Path stateFile;

    private final ObjectMapper objectMapper = new ObjectMapper();

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

        stateFile = Path.of(file.getFileName().toString() + "-upload-state.json");

        if (Files.exists(stateFile) && !resume) {
            System.err.println("Upload state file already exists: " + stateFile);
            System.err.println("Either delete it or specify --resume to continue the upload.");
            return 1;
        }

        if (resume && !Files.exists(stateFile)) {
            System.err.println("Upload state file not found: " + stateFile);
            return 1;
        }

        DirectUploadState state;
        if (resume) {
            System.err.print("Resuming upload from " + stateFile + "...");
            state = objectMapper.readValue(stateFile.toFile(), DirectUploadState.class);
            var resumeFile = Path.of(state.getFile());
            if (!file.equals(resumeFile)) {
                System.err.println("FAILED");
                System.err.println("File in upload state (" + resumeFile + ") does not match file specified on command line (" + file + ")");
                return 1;
            }
            System.err.println("OK");
        }
        else {
            long fileSize = Files.size(file);
            String sha1Checksum;
            System.err.print("Checksumming file " + file + "...");
            try (InputStream is = Files.newInputStream(file)) {
                sha1Checksum = DigestUtils.sha1Hex(is);
            }
            System.err.println("OK");
            state = DirectUploadState.builder()
                .file(file.toAbsolutePath().toString())
                .fileSize(fileSize)
                .sha1Checksum(sha1Checksum)
                .etags(new HashMap<>())
                .build();
        }

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            if (state.getUploadUrls() == null) {
                log.info("Requesting upload URLs for file size: {}", state.getFileSize());
                System.err.print("Requesting upload URLs for file size: " + state.getFileSize() + "...");
                DirectUploadURLs uploadUrls = getDatasetApi().getUploadUrls(state.getFileSize()).getData();
                state.setUploadUrls(uploadUrls);
                System.err.println("OK");
            }

            if (state.getUploadUrls().getUrl() != null) {
                System.err.println("Single part upload");
                log.info("Single part upload");
                uploadSinglePart(httpClient, state.getUploadUrls().getUrl(), file);
            }
            else if (state.getUploadUrls().getUrls() != null) {
                System.err.println("Multi part upload");
                log.info("Multi part upload");
                uploadMultiPart(httpClient, state);
            }
            else {
                throw new IllegalStateException("No upload URL(s) provided by Dataverse");
            }

            log.info("Registering file in Dataverse");
            System.err.print("Registering file in Dataverse...");
            PrestagedFile prestagedFile = new PrestagedFile();
            prestagedFile.setStorageIdentifier(state.getUploadUrls().getStorageIdentifier());
            prestagedFile.setFileName(label != null ? label : file.getFileName().toString());
            prestagedFile.setMimeType(Files.probeContentType(file));
            if (prestagedFile.getMimeType() == null) {
                prestagedFile.setMimeType("application/octet-stream");
            }
            prestagedFile.setChecksum(new Checksum("SHA-1", state.getSha1Checksum()));
            prestagedFile.setDescription(description);
            prestagedFile.setDirectoryLabel(directoryLabel);

            var response = getDatasetApi().addFile(prestagedFile);
            System.err.println("OK");
            log.debug("Response: {}", response.getEnvelopeAsString());

            if (stateFile != null && !keepUploadState) {
                Files.deleteIfExists(stateFile);
                System.err.println("Upload state file " + stateFile + " deleted");
            }

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

    private void uploadMultiPart(CloseableHttpClient httpClient, DirectUploadState state) throws IOException {
        log.info("Performing multi-part upload to S3");
        DirectUploadURLs uploadUrls = state.getUploadUrls();
        Map<String, String> etags = state.getEtags();
        long partSize = uploadUrls.getPartSize();
        Map<String, String> partUrls = uploadUrls.getUrls();
        Path file = Path.of(state.getFile());
        long fileSize = state.getFileSize();

        for (Map.Entry<String, String> entry : partUrls.entrySet()) {
            String partNumber = entry.getKey();
            String url = entry.getValue();

            if (etags.containsKey(partNumber)) {
                log.info("Part {} already uploaded, skipping", partNumber);
                System.err.println("Part " + partNumber + " of " + partUrls.size() + "...SKIPPED");
                continue;
            }

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
                objectMapper.writerWithDefaultPrettyPrinter().writeValue(stateFile.toFile(), state);
            }
            System.err.println("OK");
        }

        log.info("Completing multi-part upload");
        System.err.print("Completing multi-part upload...");
        String completeUrl = baseUrl.toString() + (baseUrl.toString().endsWith("/") ? "" : "/") + uploadUrls.getComplete();
        HttpPut completeRequest = new HttpPut(completeUrl);

        completeRequest.setHeader("X-Dataverse-key", apiToken);
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
