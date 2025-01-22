package com.fastturtle.s3uploader.controllers;

import com.fastturtle.s3uploader.utils.FileRequest;
import com.fastturtle.s3uploader.services.S3Service;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.Map;

@RestController
@RequestMapping("/api/s3")
public class S3Controller {

    private final S3Service s3Service;

    public S3Controller(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    @PostMapping("/upload")
    public Map<String, String> uploadFile(@RequestParam("file") MultipartFile file) {
        String bucketName = "bucket-for-expenses-csv";
        String fileName = file.getOriginalFilename();

        String fileUrl = s3Service.uploadFile(bucketName, fileName, file);
        return Map.of("fileUrl", fileUrl);
    }

    @PostMapping("/delete")
    public Map<String, Boolean> deleteFile(@RequestBody FileRequest fileRequest) {
        String bucketName = "bucket-for-expenses-csv";

        boolean fileDeletionResponse = s3Service.deleteFile(bucketName, fileRequest.getFileName());

        return Map.of("fileDeletionStatus", fileDeletionResponse);
    }
}
