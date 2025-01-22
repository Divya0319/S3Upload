package com.fastturtle.s3uploader.services;

import com.fastturtle.s3uploader.utils.S3UrlGenerator;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

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

    public String uploadFile(String bucketName, String fileName, MultipartFile file) {
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

        try {
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(file.getBytes()));
        } catch (IOException e) {
            throw new RuntimeException("S3 file upload error :", e);
        }

        S3UrlGenerator s3UrlGenerator = new S3UrlGenerator();

        URL presignedUrl = s3UrlGenerator.generatePreSignedUrl(bucketName, fileName, Region.AP_NORTHEAST_1);

        return presignedUrl.toString();
    }

    public boolean deleteFile(String bucketName, String fileName) {
        ListObjectVersionsRequest listRequest = ListObjectVersionsRequest.builder()
                .bucket(bucketName)
                .prefix(fileName)
                .build();

        ListObjectVersionsResponse listResponse = s3Client.listObjectVersions(listRequest);

        for(ObjectVersion version : listResponse.versions()) {
            String versionId = version.versionId();

            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .versionId(versionId)
                    .build();

            try {
                s3Client.deleteObject(deleteObjectRequest);
            } catch(Exception ex) {
                return false;
            }
        }

        return true;
    }
}
