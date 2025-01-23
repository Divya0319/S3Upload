package com.fastturtle.s3uploader.services;

import com.fastturtle.s3uploader.utils.ProgressTrackingInputStream;
import com.fastturtle.s3uploader.utils.S3UrlGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

@Service
public class S3MultipartUploadService {

    private static final long PART_SIZE = 5 * 1024 * 1024;

    private final S3Client s3Client;

    private final ConcurrentMap<Integer, CompletedPart> completedParts;

    private ScheduledExecutorService scheduler;

    private final Map<String, SseEmitter> sseEmitters = new ConcurrentHashMap<>();

    private static Logger logger = LoggerFactory.getLogger(S3MultipartUploadService.class);

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
//        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    public String multipartUpload(String bucketName, String fileName, File file) {

        Map<Integer, Long> partSizes = new ConcurrentHashMap<>();

//        startMonitoring();

        String originalFileName = fileName;

        if (fileName.endsWith(".csv") || fileName.endsWith(".xlsx")) {
            fileName = "spreadsheets/" + fileName;
        } else if (fileName.endsWith(".jpg") || fileName.endsWith(".jpeg") || fileName.endsWith(".png")) {
            fileName = "images/" + fileName;
        } else if (fileName.endsWith(".pdf")) {
            fileName = "pdfs/" + fileName;
        } else if (fileName.endsWith(".gif")) {
            fileName = "gifs/" + fileName;
        } else {
            fileName = "misc/" + fileName;
        }

        String mimeType;
        try {
            mimeType = Files.probeContentType(Paths.get(fileName));
        } catch (IOException e) {
            throw new RuntimeException("Unknown file type :", e);
        }
        if (mimeType == null) {
            mimeType = "application/octet-stream"; // Fallback for unknown file types
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
            byte[] buffer = new byte[(int) PART_SIZE];
            int bytesRead;
            int partNumber = 1;

            while ((bytesRead = fis.read(buffer)) > 0) {

//                System.out.printf("Uploading part %d, size %d bytes%n", partNumber, bytesRead);
                byte[] partBytes = new byte[bytesRead];
                System.arraycopy(buffer, 0, partBytes, 0, bytesRead);

                partSizes.put(partNumber, (long) bytesRead);

                final int currentPartNumber = partNumber++;

                String finalFileName = fileName;
                int finalBytesRead = bytesRead;
                futures.add(executor.submit(() -> {

                    InputStream progressTrackingInputStream = new ProgressTrackingInputStream(
                            new ByteArrayInputStream(partBytes),
                            currentPartNumber,
                            partSizes.get(currentPartNumber),
                            sseEmitters.get(originalFileName)
                    );
                    UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(finalFileName)
                            .uploadId(uploadId)
                            .partNumber(currentPartNumber)
                            .contentLength((long) finalBytesRead)
                            .build();


                    UploadPartResponse uploadPartResponse = s3Client.uploadPart(
                            uploadPartRequest,
                            RequestBody.fromInputStream(progressTrackingInputStream, finalBytesRead)
                    );


                    completedParts.put(currentPartNumber, CompletedPart.builder()
                            .partNumber(currentPartNumber)
                            .eTag(uploadPartResponse.eTag())
                            .build());

                    progressTrackingInputStream.close();

                    return null;
                }));


            }
        } catch (IOException e) {
            abortMultipartUpload(bucketName, fileName, uploadId);
            throw new RuntimeException("Multipart upload failed: ", e);
        }

        URL presignedUrl;
        try {

            waitForUploadCompletion(futures);

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

            removeEmitter(fileName);

            executor.shutdown();


            S3UrlGenerator s3UrlGenerator = new S3UrlGenerator();

            presignedUrl = s3UrlGenerator.generatePreSignedUrl(bucketName, fileName, Region.AP_NORTHEAST_1);

            logger.info("Multipart upload successful: {}",  fileName);
        } catch (AsyncRequestTimeoutException aex) {
            throw new RuntimeException(aex);
        }

//        stopMonitoring();

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

    // Helper methods
    private String initializeMultipartUpload(String bucketName, String fileName) {
        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .build();
        return s3Client.createMultipartUpload(request).uploadId();
    }

    private void abortMultipartUpload(String bucketName, String fileName, String uploadId) {
        AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(fileName)
                .uploadId(uploadId)
                .build();
        s3Client.abortMultipartUpload(request);
    }

    private void waitForUploadCompletion(List<Future<Void>> futures) {
        for (Future<Void> future : futures) {
            try {
                future.get(); // Wait for each part to complete
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Error while waiting for part uploads to complete: " + e.getMessage(), e);
            }
        }
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
