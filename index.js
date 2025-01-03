const { PutObjectCommand,  S3Client, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } = require("@aws-sdk/client-s3");
const fs = require("fs");
const credentials = require("./secret.json");
const path = require("path");

/**
 * @param { string } bucketName - S3 bucket name
 * @param { string } filePath - file path to upload
 * @param { string } key - S3 object key (what you want to name the file in S3)
 * @param { number } task_limit - number of tasks to run concurrently
 * @param { number } bufferSizeLimit - buffer size limit for each task
 * @param { string } region - AWS region
 * @param { number } client_max_sockets - max sockets for client, default is 50, better less then 1024
 */
const { bucketName, filePath, key, task_limit, bufferSizeLimit, region, client_max_sockets } = require("./options.json");

const fileReadStream = fs.createReadStream(path.join(filePath), { highWaterMark: bufferSizeLimit });

fileReadStream.pause();

let task_count = 0;
let upload_result = [];
let part_number = 0;
let metadata = {};
let done_size = 0;
const start_time_obj = new Date();
const start_timestamp = start_time_obj.getTime();

fileReadStream.on("data", (chunk) => {
    task_count++;
    console.log(`task[${part_number}] start`);
    uploadPart(metadata, chunk).then(([part_numberId, response]) => {
        done_size += bufferSizeLimit;
        const elapsed_time = (Date.now() - start_timestamp) / 1000;
        const speed = done_size / elapsed_time;
        console.log(`task start: ${start_time_obj.toLocaleString("sv")}`);
        console.log(`elapsed time: ${elapsed_time} s, speed: ${speed / 1024 / 1024} MB/s`);
        console.log(`upload size: ${done_size / 1024 / 1024} MB, task[${part_numberId}] done`);
        console.log("\n");
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
    region: region,
    requestHandler: {
        httpsAgent: {
            maxSockets: client_max_sockets || 50
        }
    }
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