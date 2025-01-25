package com.fastturtle.s3uploader.services;

import com.fastturtle.s3uploader.utils.ProgressTrackingInputStream;
import com.fastturtle.s3uploader.utils.S3UrlGenerator;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Service
public class S3Service {

    private final S3Client s3Client;

    private final Map<String, SseEmitter> sseEmitters = new HashMap<>();

    public S3Service(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public String uploadFile(String bucketName, String fileName, File file) {
        String mimeType;

        String originalFileName = fileName;
        try {
            mimeType = Files.probeContentType(Paths.get(fileName));
        } catch (IOException e) {
            throw new RuntimeException("Unknown file type :", e);
        }
        if (mimeType == null) {
            mimeType = "application/octet-stream"; // Fallback for unknown file types
        }

        if (fileName.endsWith(".csv") || fileName.endsWith(".xlsx")) {
            fileName = "spreadsheets/" + fileName;
        } else if (fileName.endsWith(".jpg") || fileName.endsWith(".jpeg") || fileName.endsWith(".png")) {
            fileName = "images/" + fileName;
        } else if (fileName.endsWith(".pdf")) {
            fileName = "pdfs/" + fileName;
        } else if (fileName.endsWith(".gif")) {
            fileName = "gifs/" + fileName;
        } else if(fileName.endsWith(".mp4") || fileName.endsWith(".avi") || fileName.endsWith(".m4a") || fileName.endsWith(".mkv")) {
            fileName = "videos/" + fileName;
        } else if(fileName.endsWith(".mp3") || fileName.endsWith(".wav") || fileName.endsWith(".ogg")) {
            fileName = "musics/" + fileName;
        } else {
            fileName = "misc/" + fileName;
        }

        try (InputStream fileInputStream = new FileInputStream(file);
            ProgressTrackingInputStream progressTrackingInputStream = new ProgressTrackingInputStream(
                    fileInputStream,
                    1,
                    file.length(),
                    sseEmitters.get(originalFileName)

            )){
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(fileName)
                    .contentType(mimeType) // Set the determined MIME type
                    .contentDisposition("inline") // Ensure the browser attempts to render
                    .build();

            s3Client.putObject(
                    putObjectRequest,
                    RequestBody.fromInputStream(progressTrackingInputStream, file.length()));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: ", e);
        } catch (IOException e) {
            throw new RuntimeException("IO Exception: ",e);
        }

        S3UrlGenerator s3UrlGenerator = new S3UrlGenerator();

        URL presignedUrl = s3UrlGenerator.generatePreSignedUrl(bucketName, fileName, Region.AP_NORTHEAST_1);

        removeEmitter(originalFileName);

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

    // SSE Methods
    public SseEmitter registerEmitter(String fileName) {
        SseEmitter emitter = new SseEmitter();
        sseEmitters.put(fileName, emitter);

        emitter.onCompletion(() -> sseEmitters.remove(fileName));
        emitter.onTimeout(() -> sseEmitters.remove(fileName));
        emitter.onError(e -> sseEmitters.remove(fileName));

        return emitter;
    }

    public void removeEmitter(String fileName) {
        sseEmitters.remove(fileName);
    }
}
