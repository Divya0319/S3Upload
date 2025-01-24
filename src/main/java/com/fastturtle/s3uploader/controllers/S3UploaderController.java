package com.fastturtle.s3uploader.controllers;

import com.fastturtle.s3uploader.services.S3MultipartUploadService;
import com.fastturtle.s3uploader.services.S3Service;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.File;
import java.io.IOException;

@Controller
public class S3UploaderController {

    private final S3Service s3Service;

    private final S3MultipartUploadService s3MultipartUploadService;

    public S3UploaderController(S3Service s3Service, S3MultipartUploadService s3MultipartUploadService) {
        this.s3Service = s3Service;
        this.s3MultipartUploadService = s3MultipartUploadService;
    }

    @GetMapping("/upload")
    public String showUploader() {
        return "fileUploader";
    }

    @PostMapping("/upload")
    public String uploadFile(@RequestParam("file") MultipartFile multipartFile, Model model) {

        File tempFile;
        String fileName = multipartFile.getOriginalFilename();
        try {
            tempFile = File.createTempFile("upload-", multipartFile.getOriginalFilename());
            multipartFile.transferTo(tempFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String bucketName = "bucket-for-expenses-csv";

        String uploadFileUrl;

        if(multipartFile.getSize() <= 5 * 1024 * 1024) {
            uploadFileUrl = s3Service.uploadFile(bucketName, fileName, tempFile);
        } else {
            uploadFileUrl = s3MultipartUploadService.multipartUpload(bucketName, fileName, tempFile);
        }

        String fileType = null;

        if(fileName.endsWith(".jpg") || fileName.endsWith(".jpeg")) {
            fileType = "image/jpeg";
        } else if(fileName.endsWith(".png")) {
            fileType = "image/png";
        }

        String fileCategory = fileType != null ? "image" : "other";

        model.addAttribute("uploadedFile", uploadFileUrl);
        model.addAttribute("uploadedFileType", fileCategory);

        return "fileUploader";
    }

    @GetMapping("/progress/{fileName}")
    public SseEmitter uploadProgress(@PathVariable String fileName) {
        return s3MultipartUploadService.registerEmitter(fileName);
    }
}
