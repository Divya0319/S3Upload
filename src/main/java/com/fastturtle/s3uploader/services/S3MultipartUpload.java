package com.fastturtle.s3uploader.services;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class S3MultipartUpload {

    private static final long PART_SIZE = 5 * 1024 * 1024;

    private final S3Client s3Client;

    public S3MultipartUpload(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public void multipartUpload(String bucketName, String key, File file, long fileSize) {
        CreateMultipartUploadRequest createMultipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        CreateMultipartUploadResponse createMultipartUploadResponse = s3Client.createMultipartUpload(createMultipartUploadRequest);
        String uploadId = createMultipartUploadResponse.uploadId();

        List<CompletedPart> completedParts = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[(int)PART_SIZE];
            int bytesRead;
            int partNumber = 1;

            while((bytesRead = fis.read(buffer)) > 0) {
                byte[] partBytes = new byte[bytesRead];
                System.arraycopy(buffer, 0, partBytes, 0, bytesRead);


                UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .contentLength((long)bytesRead)
                        .build();

                UploadPartResponse uploadPartResponse = s3Client.uploadPart(
                        uploadPartRequest,
                        RequestBody.fromBytes(partBytes)
                );

                completedParts.add(
                        CompletedPart.builder()
                                .partNumber(partNumber)
                                .eTag(uploadPartResponse.eTag())
                                .build()
                );

                partNumber++;
            }
        } catch (IOException e) {
            s3Client.abortMultipartUpload(
                    AbortMultipartUploadRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .build()
            );
            throw new RuntimeException("Multipart upload failed: " + e.getMessage(), e);
        }

        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                        .parts(completedParts)
                        .build())
                .build();
        s3Client.completeMultipartUpload(completeMultipartUploadRequest);

        System.out.println("Multipart upload successful: " + key);
    }
}
