package com.fastturtle.s3uploader.services;

import com.fastturtle.s3uploader.utils.S3UrlGenerator;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

@Service
public class S3Service {

    private final S3Client s3Client;

    public S3Service(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public String uploadFile(String bucketName, String fileName, String content) {
        String mimeType;
        try {
            mimeType = Files.probeContentType(Paths.get(fileName));
        } catch (IOException e) {
            throw new RuntimeException("Unknown file type :", e);
        }
        if (mimeType == null) {
            mimeType = "application/octet-stream"; // Fallback for unknown file types
        }
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .contentType(mimeType) // Set the determined MIME type
                .contentDisposition("inline") // Ensure the browser attempts to render
                .build();

        s3Client.putObject(putObjectRequest, RequestBody.fromString(content));

        S3UrlGenerator s3UrlGenerator = new S3UrlGenerator();

        URL presignedUrl = s3UrlGenerator.generatePreSignedUrl(bucketName, fileName, Region.AP_NORTHEAST_1);

        return presignedUrl.toString();
    }
}
