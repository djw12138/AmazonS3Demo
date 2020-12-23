package com.djw;

import com.djw.util.AmazonS3SimpleCode;

import java.io.IOException;

public class CommonMainApplication {
    public static void main(String[] args) {
        System.out.println("测试开始!");
        AmazonS3SimpleCode amazonS3SimpleCode=new AmazonS3SimpleCode();
        try {
            amazonS3SimpleCode.init();
            amazonS3SimpleCode.test_put_object_with_inputstream();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}