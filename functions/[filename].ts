import {
    GetObjectCommand,
    CopyObjectCommand,
    DeleteObjectCommand,
    GetObjectCommandOutput,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    AbortMultipartUploadCommand
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import mime from 'mime/lite';

import Env from './utils/Env';
import { createS3Client, auth } from './utils/utils';

export const onRequestGet: PagesFunction<Env> = async (context) => {
    const { params, env } = context;
    const { filename } = params;
    const { BUCKET } = env;
    const s3 = createS3Client(env);
    const command = new GetObjectCommand({
        Bucket: BUCKET!,
        Key: filename as string
    });
    let response: GetObjectCommandOutput;
    try {
        response = await s3.send(command);
    } catch (e) {
        return new Response("Not found", { status: 404 });
    }
    const headers = new Headers();
    for (const [key, value] of Object.entries(response.Metadata)) {
        headers.set(key, value);
    }
    if (response.ContentType !== "application/octet-stream") {
        headers.set('content-type', response.ContentType);
    } else {
        headers.set('content-type', mime.getType(filename as string) || "application/octet-stream");
    }
    if (response.Metadata['x-store-type'] === "text") {
        headers.set('content-type', 'text/plain;charset=utf-8');
    }
    headers.set('content-length', response.ContentLength.toString());
    headers.set('last-modified', response.LastModified.toUTCString());

    headers.set('etag', response.ETag);

    if (headers.get("x-store-visibility") !== "public" && !auth(env, context.request)) {
        return new Response("Not found", { status: 404 });
    }
    return new Response(
        response.Body.transformToWebStream(),
        {
            headers,
        }
    );
};

export const onRequestPut: PagesFunction<Env> = async (context) => {
    const { params, env, request } = context;
    if (!auth(env, request)) {
        return new Response("Unauthorized", { status: 401 });
    }
    const { filename } = params;
    const { BUCKET } = env;
    const s3 = createS3Client(env);
    const headers = new Headers(request.headers);
    const x_store_headers = [];
    for (const [key, value] of headers.entries()) {
        if (key.startsWith('x-store-')) {
            x_store_headers.push([key, value]);
        }
    }

    const contentType = headers.get('content-type') || mime.getType(filename as string) || "application/octet-stream";

    // Step 1: Create multipart upload
    const createMultipartUploadCommand = new CreateMultipartUploadCommand({
        Bucket: BUCKET!,
        Key: filename as string,
        ContentType: contentType,
        Metadata: Object.fromEntries(x_store_headers)
    });
    const multipartUpload = await s3.send(createMultipartUploadCommand);
    const uploadId = multipartUpload.UploadId;

    try {
        // Step 2: Upload parts
        const partSize = 1024 * 1024 * 5; // 5MB
        const parts = [];
        let partNumber = 1;
        const reader = request.body.getReader();
        let chunk;
        while (!(chunk = await reader.read()).done) {
            const uploadPartCommand = new UploadPartCommand({
                Bucket: BUCKET!,
                Key: filename as string,
                PartNumber: partNumber,
                UploadId: uploadId,
                Body: chunk.value
            });
            const part = await s3.send(uploadPartCommand);
            parts.push({ PartNumber: partNumber, ETag: part.ETag });
            partNumber++;
        }

        // Step 3: Complete multipart upload
        const completeMultipartUploadCommand = new CompleteMultipartUploadCommand({
            Bucket: BUCKET!,
            Key: filename as string,
            UploadId: uploadId,
            MultipartUpload: { Parts: parts }
        });
        await s3.send(completeMultipartUploadCommand);

        return new Response("OK", { status: 200 });
    } catch (error) {
        // Abort multipart upload on error
        const abortMultipartUploadCommand = new AbortMultipartUploadCommand({
            Bucket: BUCKET!,
            Key: filename as string,
            UploadId: uploadId
        });
        await s3.send(abortMultipartUploadCommand);
        throw error;
    }
};

export const onRequestPatch: PagesFunction<Env> = async (context) => {
    const { params, env, request } = context;
    if (!auth(env, request)) {
        return new Response("Unauthorized", { status: 401 });
    }
    const { filename } = params;
    const { BUCKET } = env;
    const s3 = createS3Client(env);
    const headers = new Headers(request.headers);
    const x_store_headers = [];
    for (const [key, value] of headers.entries()) {
        if (key.startsWith('x-store-')) {
            x_store_headers.push([key, value]);
        }
    }
    const command = new CopyObjectCommand({
        Bucket: BUCKET!,
        CopySource: `${BUCKET}/${filename}`,
        Key: filename as string,
        MetadataDirective: "REPLACE",
        Metadata: Object.fromEntries(x_store_headers),
    });
    await s3.send(command);
    return new Response("OK", { status: 200 });
};

export const onRequestDelete: PagesFunction<Env> = async (context) => {
    const { params, env, request } = context;
    if (!auth(env, request)) {
        return new Response("Unauthorized", { status: 401 });
    }
    const { filename } = params;
    const { BUCKET } = env;
    const s3 = createS3Client(env);
    const command = new DeleteObjectCommand({
        Bucket: BUCKET!,
        Key: filename as string
    });
    const url = await getSignedUrl(
        s3,
        command,
        { expiresIn: 3600 }
    );
    await fetch(url, {
        method: 'DELETE',
    });
    return new Response("OK", { status: 200 });
}

export const onRequest: PagesFunction<Env> = async () => {
    return new Response("Method not allowed", { status: 405 });
};
