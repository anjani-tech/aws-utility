import { S3Event, S3Handler } from 'aws-lambda';
import { S3 } from 'aws-sdk';
import { createHash } from 'crypto';

const s3 = new S3();

const calculateSHA256 = (data: Buffer): string => {
  const hash = createHash('sha256');
  hash.update(data);
  return hash.digest('hex');
};

export const handler: S3Handler = async (event: S3Event) => {
  try {
    for (const record of event.Records) {
      const s3Object = record.s3;
      const bucketName = s3Object.bucket.name;
      const objectKey = decodeURIComponent(s3Object.object.key.replace(/\+/g, ' '));
      
      // Retrieve object metadata
      const getObjectParams: S3.GetObjectRequest = {
        Bucket: bucketName,
        Key: objectKey,
      };
      const { Metadata } = await s3.getObject(getObjectParams).promise();
      
      // Calculate SHA256 hash
      const getObjectDataParams: S3.GetObjectRequest = {
        Bucket: bucketName,
        Key: objectKey,
      };
      const { Body } = await s3.getObject(getObjectDataParams).promise();
      if (Body) {
        const sha256 = calculateSHA256(Body as Buffer);
        // Add SHA256 hash to metadata
        const updatedMetadata = {
          ...Metadata,
          'sha256': sha256,
        };
        
        // Update object metadata
        const copyObjectParams: S3.CopyObjectRequest = {
          Bucket: bucketName,
          Key: objectKey,
          CopySource: `${bucketName}/${objectKey}`,
          MetadataDirective: 'REPLACE',
          Metadata: updatedMetadata,
        };
        await s3.copyObject(copyObjectParams).promise();
        
        console.log(`Added SHA256 hash (${sha256}) to metadata for object: ${objectKey}`);
      } else {
        console.error(`Failed to retrieve object data for: ${objectKey}`);
      }
    }
  } catch (error) {
    console.error('Error processing S3 event:', error);
    throw error;
  }
};
