package com.fastturtle.s3uploader.utils;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ProgressTrackingInputStream extends InputStream {

    private final InputStream delegate;
    private final int partNumber;
    private final long totalBytes;
    private final SseEmitter emitter;
    private long bytesUploaded;
    private double lastSentPercentage;

    public ProgressTrackingInputStream(InputStream delegate, int partNumber, long totalBytes, SseEmitter emitter) {
        this.delegate = delegate;
        this.partNumber = partNumber;
        this.totalBytes = totalBytes;
        this.emitter = emitter;
        this.bytesUploaded = 0;
        this.lastSentPercentage = 0;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int bytesRead = delegate.read(b, off, len);
        if(bytesRead > 0) {
            bytesUploaded += bytesRead;

            double percentage = (double)bytesUploaded / totalBytes * 100;


            if(percentage - lastSentPercentage >= 5) {
                sendPartProgress(partNumber, bytesUploaded, totalBytes);
                lastSentPercentage = percentage;
            }

        }

        return bytesRead;
    }

    @Override
    public int read() throws IOException {
        int byteRead = delegate.read();
        if(byteRead != -1) {
            bytesUploaded++;
        }
        // Calculate percentage
        double percentage = (double) bytesUploaded / totalBytes * 100;

        // Send update only if progress changed by at least 5%
        if (percentage - lastSentPercentage >= 5) {
            sendPartProgress(partNumber, bytesUploaded, totalBytes);
            lastSentPercentage = percentage;
        }

        return byteRead;
    }

    public void sendPartProgress(int partNumber, long bytesUploaded, long partSize) {
        double percentage = ((double) bytesUploaded / partSize) * 100;
        if (emitter != null) {
            try {
                Map<String, Object> progressData = new HashMap<>();
                progressData.put("partNumber", partNumber);
                progressData.put("percentage", percentage);

                emitter.send(progressData);
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
        }
    }
}
