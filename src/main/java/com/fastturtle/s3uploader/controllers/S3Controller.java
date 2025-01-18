package com.fastturtle.s3uploader.controllers;

import com.fastturtle.s3uploader.services.S3Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("/api/s3")
public class S3Controller {

    private final S3Service s3Service;

    public S3Controller(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    @PostMapping("/upload")
    public String uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            String bucketName = "bucket-for-expenses-csv";
            String fileName = file.getOriginalFilename();

            s3Service.uploadFile(bucketName, fileName, new String(file.getBytes()));
            return "File uploaded successfully";
        } catch (IOException e) {
            return "Error occurred: " + e.getMessage();
        }
    }
}
