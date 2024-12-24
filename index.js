const { PutObjectCommand,  S3Client, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } = require("@aws-sdk/client-s3");
const fs = require("fs");
const credentials = require("./secret.json");

const { bucketName, key, task_limit, bufferSizeLimit, region } = require("./config.json");

const fileReadStream = fs.createReadStream(key, { highWaterMark: bufferSizeLimit });

fileReadStream.pause();

let task_count = 0;
let upload_result = [];
let part_number = 0;
let metadata = {};

fileReadStream.on("data", (chunk) => {
    task_count++;
    uploadPart(metadata, chunk).then(([part_numberId, response]) => {
        upload_result[part_numberId] = response;
        task_count--;
        if (task_count < task_limit) {
            fileReadStream.resume();
        }
    }).catch((error) => {
        console.log("file upload error", error);
        task_count--;
        if (task_count < task_limit) {
            fileReadStream.resume();
        }
    });
    if (task_count > task_limit) {
        fileReadStream.pause();
    }
});

async function waitUntilTaskDone() {
    return new Promise((resolve) => {
        const interval = setInterval(() => {
            if (task_count === 0) {
                clearInterval(interval);
                resolve();
            }
        }, 1000);
    });
}

fileReadStream.on("end", async () => {
    await waitUntilTaskDone();
    const completeMultipartUploadCommand = new CompleteMultipartUploadCommand({
        Bucket: bucketName,
        Key: key,
        UploadId: metadata.UploadId,
        MultipartUpload: {
            Parts: upload_result.map((part, index) => {
                return { PartNumber: index + 1, ETag: part.ETag, ChecksumSHA256: part.ChecksumSHA256 }
            })
        },
        ChecksumSHA256: metadata.Checksum
    });
    client.send(completeMultipartUploadCommand).then((response) => {
        console.log(response);
    }).catch((error) => {
        console.log("complete error", error);
    });
})


const client = new S3Client({
    credentials: credentials,
    region: region
});

const command = new CreateMultipartUploadCommand({
    Bucket: bucketName,
    Key: key,
    ChecksumAlgorithm: "SHA256"
});

async function uploadPart(metadata, chunk) {
    let part_numberId = part_number++;
    const uploadPartCommand = new UploadPartCommand({
        Bucket: bucketName,
        Key: key,
        UploadId: metadata.UploadId,
        PartNumber: part_numberId + 1,
        Body: chunk,
        ChecksumAlgorithm: "SHA256",
        ChecksumSHA256: metadata.Checksum
    });
    const response = await client.send(uploadPartCommand);
    return [part_numberId, response];
}





async function init() {
    try {
        const multipartUpload = await client.send(command);
        metadata = multipartUpload;
        fileReadStream.resume();
    } catch (error) {
        console.log(error);
    }
}

init().then(() => {
    console.log('init done')
}).catch((erorr) => {
    console.log(erorr)
})