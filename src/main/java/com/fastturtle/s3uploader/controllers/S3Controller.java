package com.fastturtle.s3uploader.controllers;

import com.fastturtle.s3uploader.services.S3MultipartUpload;
import com.fastturtle.s3uploader.utils.FileRequest;
import com.fastturtle.s3uploader.services.S3Service;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping("/api/s3")
public class S3Controller {

    private final S3Service s3Service;

    private final S3MultipartUpload s3MultipartUpload;

    public S3Controller(S3Service s3Service, S3MultipartUpload s3MultipartUpload) {
        this.s3Service = s3Service;
        this.s3MultipartUpload = s3MultipartUpload;
    }

    @PostMapping("/upload")
    public Map<String, String> uploadFile(@RequestParam("file") MultipartFile multipartFile) {

        File tempFile;
        String fileName = multipartFile.getOriginalFilename();
        try {
            tempFile = File.createTempFile("upload-", multipartFile.getOriginalFilename());
            multipartFile.transferTo(tempFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String bucketName = "bucket-for-expenses-csv";
        String fileUrl;

        if(multipartFile.getSize() <= 1024 * 1024) {
            fileUrl = s3Service.uploadFile(bucketName, fileName, tempFile);
        } else {
            fileUrl = s3MultipartUpload.multipartUpload(bucketName, fileName, tempFile);
        }
        return Map.of("fileUrl", fileUrl);
    }

    @PostMapping("/delete")
    public Map<String, Boolean> deleteFile(@RequestBody FileRequest fileRequest) {
        String bucketName = "bucket-for-expenses-csv";

        boolean fileDeletionResponse = s3Service.deleteFile(bucketName, fileRequest.getFileName());

        return Map.of("fileDeletionStatus", fileDeletionResponse);
    }
}
