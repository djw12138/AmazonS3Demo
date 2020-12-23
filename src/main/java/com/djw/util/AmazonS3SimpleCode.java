package com.djw.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;

import java.io.*;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.util.Date;


public class AmazonS3SimpleCode {
    private String endPoint = "http://10.62.1.8:7480";
    private String accessKey = "FOHLHJ58GULE050V9KG8";
    private String secretKey = "ARkbaUNnDPhxumjDjXWLhd4wskqRSo9IUvt5Ui7N";
    private String bucketName = "audio-video-bucket-1";
    private String objectKey = "objecttest";
    private AmazonS3 s3Client;


    /**
     * 初始化Amazon S3 Client
     */
    public void init() {
        AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setMaxErrorRetry(3);
        clientConfig.setConnectionTimeout(20 * 1000);
        clientConfig.setMaxConnections(10);
        clientConfig.setSignerOverride("S3SignerType");
        clientConfig.setProtocol(Protocol.HTTP);
        s3Client = new AmazonS3Client(awsCredentials, clientConfig);
        s3Client.setEndpoint(endPoint);
        //使用域名时，需要如下设置
        s3Client.setS3ClientOptions(S3ClientOptions.builder().setPathStyleAccess(true).build());
    }

    /**
     * 基于文件PATH路径上传object
     *
     * @throws IOException
     */
    public void test_put_object_with_file() throws IOException {
        /*在临时目录准备一个上传测试文件*/
        String filePath = prepareAFile();

        PutObjectResult putObjectResult = s3Client.putObject(bucketName, objectKey, new File(filePath));
        Assert.assertTrue(putObjectResult.getETag() != null);
    }

    /**
     * 基于文件流上传object
     *
     * @throws IOException
     */
    public void test_put_object_with_inputstream() throws IOException {
        /*准备一个Inputstream*/
        String objectContent = "testobject";
        InputStream in = prepareAInputstream(objectContent);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(objectContent.length());
        metadata.setContentEncoding("UTF-8");
        metadata.addUserMetadata("userkey1", "key_value");
        metadata.addUserMetadata("testcn", URLEncoder.encode("中文测试", "UTF-8"));
        PutObjectResult putObjectResult =s3Client.putObject(bucketName, objectKey, in, metadata);
        System.out.println(putObjectResult.toString());
        System.out.println(String.valueOf(putObjectResult));
        System.out.println(putObjectResult.getMetadata().toString());
    }

    /**
     * 基于字符串上传object
     */
    public void test_put_object_with_string() {
        PutObjectResult putObjectResult = s3Client.putObject(bucketName, objectKey, "testobject");
        Assert.assertTrue(putObjectResult.getETag() != null);
    }

    /**
     * 基于request model上传object
     */
    public void test_put_object_with_request() throws UnsupportedEncodingException {
        /*准备一个Inputstream*/
        String objectContent = "testobject";
        InputStream in = prepareAInputstream(objectContent);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(objectContent.length());
        metadata.setContentEncoding("UTF-8");
        metadata.addUserMetadata("userkey1", "key_value1");
        metadata.addUserMetadata("testcn", URLEncoder.encode("中文测试", "UTF-8"));
        PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, in, metadata);
        s3Client.putObject(request);
    }

    /**
     * 基于objectKey获取object
     *
     * @throws IOException
     */
    public void test_get_object_key() throws IOException {
        /*准备一个object*/
        this.test_put_object_with_inputstream();

        S3Object s3Object = s3Client.getObject(bucketName, objectKey);
        System.out.println(s3Object.getKey());
    }

    /**
     * 基于object Request model获取object
     *
     * @throws IOException
     */
    public void test_get_object_request() throws IOException {
        /*准备一个上传object*/
        this.test_put_object_with_inputstream();

        GetObjectRequest getRequest = new GetObjectRequest(bucketName, objectKey);
        S3Object s3Object = s3Client.getObject(getRequest);
        System.out.println(s3Object.getKey());
    }

    /**
     * 基于object Request Model获取object并存储到指定文件对象
     *
     * @throws IOException
     */
    public void test_get_object_request_destfile() throws IOException {
        /*准备一个上传object*/
        this.test_put_object_with_inputstream();
        /*创建一个存储的目标文件*/
        File destinationFile = new File(System.getProperty("java.io.tmpdir") + File.separator + "test_get_object_request_destfile");

        GetObjectRequest getRequest = new GetObjectRequest(bucketName, objectKey);
        ObjectMetadata objectMetadata = s3Client.getObject(getRequest, destinationFile);
        System.out.println(objectMetadata.getETag());
    }

    /**
     * 通过object key获取元数据
     */
    public void test_get_object_metadata() throws IOException {
        /*准备一个上传object*/
        this.test_put_object_with_inputstream();

        ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucketName, objectKey);
        System.out.println(objectMetadata.getETag());
    }

    /**
     * 通过object key获取元数据
     */
    public void test_get_object_metadata_request() throws IOException {
        /*准备一个上传object*/
        this.test_put_object_with_inputstream();

        GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(bucketName, objectKey);
        ObjectMetadata objectMetadata = s3Client.getObjectMetadata(metadataRequest);
        System.out.println(objectMetadata.getETag());
    }

    /**
     * 通过Object Key 删除一个对象
     */
    public void test_delete_object() throws IOException {
        s3Client.deleteObject(bucketName, objectKey);
    }


    /**
     * 通过ObjectRequest 删除一个对象
     */
    public void test_delete_object_request() throws IOException {
        DeleteObjectRequest request = new DeleteObjectRequest(bucketName, objectKey);
        s3Client.deleteObject(request);
    }

    /**
     * 通过Object Key和超时时间，在本地签名一个URL
     * 此URL可以直接通过HTTP请求获取文件对象二进制流
     * 特别适用于图片，可以直接通过此URL在浏览器打开
     */
    public void test_generate_presignedurl() {
        /*设置5分钟过期*/
        Date dateTime = new Date();
        long msec = dateTime.getTime();
        msec += 1000 * 60 * 5;
        dateTime.setTime(msec);

        URL url = s3Client.generatePresignedUrl(bucketName, objectKey, dateTime);
        System.out.println(url);
    }


    /**
     * list 一个bucket下的object列表（1000条）
     */
    public void test_list_object() {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucketName);

        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
        System.out.println(objectListing.getObjectSummaries());
    }

    public void test_list_object_request() {
        /*进行第一批Object list*/
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucketName);
        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

        /*进行第二批次Object list*/
        ObjectListing nextObjectListing = s3Client.listNextBatchOfObjects(objectListing);
        System.out.println(nextObjectListing.getObjectSummaries());
    }

    public void test_multipart_Upload() throws IOException {
        /*准备一个大文件*/
        File bigFile = prepareABigFile(200);

        String bigFileObject = "bigFileObject";
        TransferManager tm = new TransferManager(s3Client);
        Upload upload = tm.upload(bucketName, bigFileObject, bigFile);
        try {
            upload.waitForCompletion();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void test_multipart_Upload_inputstream() throws IOException {
        /*准备一个大文件*/
        File bigFile = prepareABigFile(200);

        String bigFileObject = "bigFileObject";
        TransferManager tm = new TransferManager(s3Client);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bigFile.length());
        metadata.setContentEncoding("UTF-8");
        metadata.addUserMetadata("userkey1", "key_value1");
        metadata.addUserMetadata("testcn", URLEncoder.encode("中文测试", "UTF-8"));
        Upload upload = tm.upload(bucketName, bigFileObject, new FileInputStream(bigFile), metadata);
        try {
            upload.waitForCompletion();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private File prepareABigFile(int mb) throws IOException {
        int size = mb * 1024 * 1024;
        String filePath = getTempFilePath();
        File file = new File(filePath);
        byte[] buff = new byte[size];
        FileUtils.writeStringToFile(file, new String(buff));
        return file;
    }

    private String getDigest(File file) throws IOException {
        MessageDigest digest = DigestUtils.getMd5Digest();
        digest.update(IOUtils.toByteArray(new FileInputStream(file)));
        BigInteger bi = new BigInteger(1, digest.digest());
        return bi.toString(16);
    }


    private InputStream prepareAInputstream(String conetnt) {
        ByteArrayInputStream byteInput = new ByteArrayInputStream(conetnt.getBytes());
        return byteInput;
    }


    private String prepareAFile() throws IOException {
        String filePath = getTempFilePath();
        FileUtils.writeStringToFile(new File(filePath), "testobject");
        return filePath;
    }

    private void deleteTempFile() {
        String filePath = getTempFilePath();
        File file = new File(filePath);
        file.delete();
    }

    private String getTempFilePath() {
        return System.getProperty("java.io.tmpdir") + File.separator + "s3testdemofile";
    }


}
