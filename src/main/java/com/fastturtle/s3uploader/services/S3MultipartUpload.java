package com.fastturtle.s3uploader.services;

import com.fastturtle.s3uploader.utils.S3UrlGenerator;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class S3MultipartUpload {

    private static final long PART_SIZE = 5 * 1024 * 1024;

    private final S3Client s3Client;

    public S3MultipartUpload(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public String multipartUpload(String bucketName, String fileName, File file) {

        String mimeType;
        try {
            mimeType = Files.probeContentType(Paths.get(fileName));
        } catch (IOException e) {
            throw new RuntimeException("Unknown file type :", e);
        }
        if (mimeType == null) {
            mimeType = "application/octet-stream"; // Fallback for unknown file types
        }

        if(fileName.endsWith(".csv") || fileName.endsWith(".xlsx")) {
            fileName = "spreadsheets/" + fileName;
        } else if(fileName.endsWith(".jpg") || fileName.endsWith(".jpeg") || fileName.endsWith(".png")) {
            fileName = "images/" + fileName;
        } else if(fileName.endsWith(".pdf")) {
            fileName = "pdfs/" + fileName;
        } else if(fileName.endsWith(".gif")) {
            fileName = "gifs/" + fileName;
        } else {
            fileName = "misc/" + fileName;
        }

        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .contentType(mimeType)
                .contentDisposition("inline")
                .build();

        CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest);
        String uploadId = createMultipartUploadResponse.uploadId();

        // Utilising multithreading to upload multiple parts concurrently
        ExecutorService executor = Executors.newFixedThreadPool(4);

        List<Future<CompletedPart>> futures = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[(int)PART_SIZE];
            int bytesRead;
            int partNumber = 1;

            while((bytesRead = fis.read(buffer)) > 0) {

                System.out.printf("Uploading part %d, size %d bytes%n", partNumber, bytesRead);
                byte[] partBytes = new byte[bytesRead];
                System.arraycopy(buffer, 0, partBytes, 0, bytesRead);

                final int currentPartNumber = partNumber++;
                final byte[] currentPartData = partBytes;

                String finalFileName = fileName;
                int finalPartNumber = partNumber;
                int finalBytesRead = bytesRead;
                futures.add(executor.submit(() -> {
                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(finalFileName)
                            .uploadId(uploadId)
                            .partNumber(finalPartNumber)
                            .contentLength((long) finalBytesRead)
                            .build();


                    UploadPartResponse uploadPartResponse = s3Client.uploadPart(
                            uploadPartRequest,
                            RequestBody.fromBytes(currentPartData)
                    );

                    return CompletedPart.builder()
                            .partNumber(currentPartNumber)
                            .eTag(uploadPartResponse.eTag())
                            .build();
                }));

            }
        } catch (IOException e) {
            s3Client.abortMultipartUpload(
                    AbortMultipartUploadRequest.builder()
                            .bucket(bucketName)
                            .key(fileName)
                            .uploadId(uploadId)
                            .build()
            );
            throw new RuntimeException("Multipart upload failed: " + e.getMessage(), e);
        }

        List<CompletedPart> completedParts = new ArrayList<>();
        try {
            for(Future<CompletedPart> future : futures) {

                completedParts.add(future.get());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("InterruptedException: " + e);
        } catch (ExecutionException e) {
            throw new RuntimeException("ExecutionException:" + e);
        }

        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                .parts(completedParts)
                .build();

        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .uploadId(uploadId)
                .multipartUpload(completedMultipartUpload)
                .build();

        s3Client.completeMultipartUpload(completeMultipartUploadRequest);

        executor.shutdown();

        S3UrlGenerator s3UrlGenerator = new S3UrlGenerator();

        URL presignedUrl = s3UrlGenerator.generatePreSignedUrl(bucketName, fileName, Region.AP_NORTHEAST_1);

        System.out.println("Multipart upload successful: " + fileName);

        return presignedUrl.toString();
    }
}
