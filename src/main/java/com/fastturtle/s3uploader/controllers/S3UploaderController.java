package com.fastturtle.s3uploader.controllers;

import com.fastturtle.s3uploader.services.S3Service;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@Controller
public class S3UploaderController {

    private final S3Service s3Service;

    public S3UploaderController(S3Service s3Service) {
        this.s3Service = s3Service;
    }

    @GetMapping("/upload")
    public String showUploader() {
        return "fileUploader";
    }

    @PostMapping("/upload")
    public String uploadFile(@RequestParam("file") MultipartFile file, Model model) {
        String bucketName = "bucket-for-expenses-csv";
        String fileName = file.getOriginalFilename();

        String uploadFileUrl = s3Service.uploadFile(bucketName, fileName, file);

        String fileType = file.getContentType();
        String fileCategory = fileType != null && fileType.startsWith("image") ? "image" : "other";

        model.addAttribute("uploadedFile", uploadFileUrl);
        model.addAttribute("uploadedFileType", fileCategory);

        return "fileUploader";
    }
}
