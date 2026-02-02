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

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.Callable;

@Command(name = "deposits-create-report",
    description = "Create report based on dd-manage-deposit database",
    mixinStandardHelpOptions = true)
@Slf4j
public class DepositsCreateReport implements Callable<Integer> {

    @Option(names = {"-b", "--service-baseurl"}, required = true, description = "Base URL of dd-manage-deposit service, e.g., http://host:port")
    private String serviceBaseUrl;

    @Option(names = {"-o", "--output-file"}, description = "Output file path or '-' for stdout", defaultValue = "-")
    private String outputFile;

    @Option(names = {"-e", "--enddate"}, description = "Filter until the record creation of this date (YYYY-MM-DD)")
    private String enddate;

    @Option(names = {"-s", "--startdate"}, description = "Filter from the record creation of this date (YYYY-MM-DD)")
    private String startdate;

    @Option(names = {"-a", "--age"}, description = "Filter records not older than this many days before today")
    private Integer age;

    @Option(names = {"-t", "--state"}, description = "The state of the deposit (repeatable)", split = ",")
    private List<String> state = new ArrayList<>();

    @Option(names = {"-u", "--user"}, description = "The depositor name (repeatable)", split = ",")
    private List<String> user = new ArrayList<>();

    @Option(names = {"-f", "--format"}, description = "Output data format for Accept header", defaultValue = "text/csv")
    private String fileFormat;

    @Option(names = {"-v", "--server"}, description = "Server label for email subject (prod/demo/..)", defaultValue = "unknown server")
    private String server;

    @Option(names = {"-r", "--from"}, description = "From address for emailing the report")
    private String emailFromAddress;

    @Option(names = {"--email-to"}, description = "Recipient email(s), comma-separated")
    private String emailTo;

    @Option(names = {"--cc-email-to"}, description = "CC email(s), comma-separated")
    private String ccEmailTo;

    @Option(names = {"--bcc-email-to"}, description = "BCC email(s), comma-separated")
    private String bccEmailTo;

    @Override
    public Integer call() throws Exception {
        // Validate mutually exclusive: startdate vs age (but allow neither)
        if (startdate != null && age != null) {
            System.err.println("Specify either --startdate or --age, not both");
            return 2;
        }

        // Compute startdate from age if provided
        if (age != null) {
            LocalDate computed = LocalDate.now().minusDays(age);
            startdate = computed.format(DateTimeFormatter.ISO_DATE);
        }

        String url = normalizeBaseUrl(serviceBaseUrl) + "/report";
        String query = buildQueryParams();
        URI uri = URI.create(url + (query.isEmpty() ? "" : ("?" + query)));

        log.debug("Requesting report from {}", uri);
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder(uri)
            .GET()
            .header("Accept", fileFormat);
        HttpRequest request = reqBuilder.build();

        HttpResponse<String> response;
        try {
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException | InterruptedException e) {
            System.err.println("Error contacting manage-deposit service: " + e.getMessage());
            return 1;
        }

        if (response.statusCode() == 200) {
            String report = response.body();
            return handleReport(report);
        }
        else {
            System.err.printf("ManageDeposit:create_report() - HTTP %d%n", response.statusCode());
            return 1;
        }
    }

    private String buildQueryParams() {
        StringJoiner joiner = new StringJoiner("&");
        if (!user.isEmpty()) {
            for (String u : user) {
                joiner.add("user=" + enc(u));
            }
        }
        if (!state.isEmpty()) {
            for (String s : state) {
                joiner.add("state=" + enc(s));
            }
        }
        if (startdate != null && !startdate.isBlank()) {
            joiner.add("startdate=" + enc(startdate));
        }
        if (enddate != null && !enddate.isBlank()) {
            joiner.add("enddate=" + enc(enddate));
        }
        return joiner.toString();
    }

    private String enc(String v) {
        return URLEncoder.encode(v, StandardCharsets.UTF_8);
    }

    private int handleReport(String report) {
        if ("-".equals(outputFile)) {
            System.out.println(report);
            return 0;
        }
        // If multi-line, write to file like Python implementation
        if (report != null && report.split("\n").length > 1) {
            try {
                Path out = Path.of(outputFile);
                Files.createDirectories(out.getParent() != null ? out.getParent() : Path.of("."));
                Files.writeString(out, report);
                System.out.printf("Report written to %s%n", outputFile);
            }
            catch (IOException e) {
                System.err.println("Failed to write report: " + e.getMessage());
                return 1;
            }
            if (emailTo != null && !emailTo.isBlank()) {
                int rc = sendReportMail(outputFile);
                if (rc != 0) {
                    System.err.println("Failed to send email (mail utility returned non-zero exit code)");
                    return rc;
                }
            }
            return 0;
        }
        else {
            System.out.println("Report is empty.");
            return 0;
        }
    }

    private int sendReportMail(String attachment) {
        // Compose the command similarly to Python's SendMail.send
        String subject = String.format("Deposits report (%s)", server.toUpperCase());
        String messageBody = "Please, find attached the detailed report of deposits.";

        List<String> cmd = new ArrayList<>();
        // Use zsh -c to process the pipeline echo ... | mail ...
        StringBuilder sb = new StringBuilder();
        sb.append("echo ");
        sb.append(shellQuote(messageBody));
        sb.append(" | mail -s ");
        sb.append(shellQuote(subject));
        if (attachment != null && !attachment.isBlank()) {
            sb.append(" -a ");
            sb.append(shellQuote(attachment));
        }
        if (emailFromAddress != null && !emailFromAddress.isBlank()) {
            sb.append(" -r ");
            sb.append(shellQuote(emailFromAddress));
        }
        String recipients = emailTo;
        if (ccEmailTo != null && !ccEmailTo.isBlank()) {
            sb.append(" -c ");
            sb.append(shellQuote(ccEmailTo));
        }
        if (bccEmailTo != null && !bccEmailTo.isBlank()) {
            sb.append(" -b ");
            sb.append(shellQuote(bccEmailTo));
        }
        if (recipients != null) {
            sb.append(" ");
            sb.append(recipients);
        }

        cmd.add("sh");
        cmd.add("-c");
        cmd.add(sb.toString());

        log.debug("Executing mail command: {}", String.join(" ", cmd));

        try {
            Process process = new ProcessBuilder(cmd).inheritIO().start();
            return process.waitFor();
        }
        catch (IOException | InterruptedException e) {
            System.err.println("Error executing mail command: " + e.getMessage());
            return 1;
        }
    }

    private String shellQuote(String s) {
        // Simple single-quote wrapper with escaping of single quotes
        if (s == null) return "''";
        return "'" + s.replace("'", "'\\''") + "'";
    }

    private String normalizeBaseUrl(String base) {
        if (base.endsWith("/")) {
            return base.substring(0, base.length() - 1);
        }
        return base;
    }
}
