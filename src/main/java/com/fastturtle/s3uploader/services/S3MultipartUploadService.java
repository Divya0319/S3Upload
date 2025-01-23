package com.fastturtle.s3uploader.services;

import com.fastturtle.s3uploader.utils.S3UrlGenerator;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class S3MultipartUploadService {

    private static final long PART_SIZE = 5 * 1024 * 1024;

    private final S3Client s3Client;

    private final ConcurrentMap<Integer, CompletedPart> completedParts;

    private ScheduledExecutorService scheduler;

    // Utilising multithreading to upload multiple parts concurrently
    private ExecutorService executor = new ThreadPoolExecutor(
            4,
            10,
            60L,
            TimeUnit.SECONDS,
            new LinkedBlockingDeque<>());

    public S3MultipartUploadService(S3Client s3Client) {
        this.s3Client = s3Client;
        completedParts = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    public String multipartUpload(String bucketName, String fileName, File file, SseEmitter emitter) {

        long totalFileSize = file.length();

        AtomicLong uploadedBytes = new AtomicLong(0);

        startMonitoring();

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

        List<Future<Void>> futures = new ArrayList<>();
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
                int finalBytesRead = bytesRead;
                futures.add(executor.submit(() -> {
                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(finalFileName)
                            .uploadId(uploadId)
                            .partNumber(currentPartNumber)
                            .contentLength((long) finalBytesRead)
                            .build();


                    UploadPartResponse uploadPartResponse = s3Client.uploadPart(
                            uploadPartRequest,
                            RequestBody.fromBytes(currentPartData)
                    );

                    long uploaded = uploadedBytes.addAndGet(finalBytesRead);
                    int progress = (int)((uploaded * 100) / totalFileSize);

                    emitter.send("Part " + currentPartNumber + ": " + progress + "% complete");

                    completedParts.put(currentPartNumber,CompletedPart.builder()
                            .partNumber(currentPartNumber)
                            .eTag(uploadPartResponse.eTag())
                            .build());
                    return null;
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
            throw new RuntimeException("Multipart upload failed: ", e);
        }

        try {
            for(Future<Void> future : futures) {
                future.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Error while waiting for parts to upload", e);
        }

        // Sorting completed parts by partNumber
        List<CompletedPart> sortedCompletedParts = new ArrayList<>(completedParts.values());
        sortedCompletedParts.sort(Comparator.comparingInt(CompletedPart::partNumber));

        CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                .parts(sortedCompletedParts)
                .build();

        CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .uploadId(uploadId)
                .multipartUpload(completedMultipartUpload)
                .build();

        s3Client.completeMultipartUpload(completeMultipartUploadRequest);

        try {
            emitter.send("Upload complete");
        } catch (IOException e) {
            throw new RuntimeException("Emitter send error: ", e);
        } finally {
            emitter.complete();
            executor.shutdown();
        }

        S3UrlGenerator s3UrlGenerator = new S3UrlGenerator();

        URL presignedUrl = s3UrlGenerator.generatePreSignedUrl(bucketName, fileName, Region.AP_NORTHEAST_1);

        System.out.println("Multipart upload successful: " + fileName);

        stopMonitoring();

        return presignedUrl.toString();
    }

    private void startMonitoring() {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("===== Thread Pool Stats =====");
            System.out.println("Active threads: " + threadPoolExecutor.getActiveCount());
            System.out.println("Pool size: " + threadPoolExecutor.getPoolSize());
            System.out.println("Largest pool size: " + threadPoolExecutor.getLargestPoolSize());
            System.out.println("Completed tasks: " + threadPoolExecutor.getCompletedTaskCount());
            System.out.println("Task queue size: " + threadPoolExecutor.getQueue().size());
            System.out.println("=============================");
        }, 0, 5, TimeUnit.SECONDS); // Logs every 5 seconds
    }

    private void stopMonitoring() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
    }
}
