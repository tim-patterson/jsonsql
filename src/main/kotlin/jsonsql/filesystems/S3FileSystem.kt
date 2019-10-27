package jsonsql.filesystems

import com.amazonaws.SdkClientException
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.S3ObjectSummary
import jsonsql.functions.StringInspector
import java.io.*
import java.net.URI


object S3FileSystem: StreamFileSystem() {

    private val s3 = AmazonS3ClientBuilder.standard()
            .withForceGlobalBucketAccessEnabled(true)
            .withRegion(try { DefaultAwsRegionProviderChain().region } catch (e: SdkClientException) { "ap-southeast-2" })
            .build()

    override fun listDir(path: String): Sequence<Map<String, Any?>> {
        val s3Uri = URI.create(path)
        val authority = s3Uri.authority
        val prefix = s3Uri.path.trimStart('/')

        var listing = s3.listObjects(authority, prefix)

        return generateSequence(listing) {
            listing = s3.listNextBatchOfObjects(listing)
            if(listing.isTruncated) listing else null
        }.flatMap { it.objectSummaries.asSequence() }
        .map {
            mapOf(
                    "path" to "s3://$authority/${it.key}",
                    "bucket" to it.bucketName,
                    "key" to it.key,
                    "owner" to it.owner?.displayName,
                    "storage_class" to it.storageClass,
                    "last_modified" to StringInspector.inspect(it.lastModified.toInstant()),
                    "size" to it.size
            )
        }
    }


    override fun readSingle(file: Map<String, Any?>): InputStream {
        return s3.getObject(file["bucket"] as String, file["key"] as String).objectContent
    }

    override fun write(path: String): OutputStream {
        val s3Uri = URI.create(path)
        val authority = s3Uri.authority
        val key = s3Uri.path.trimStart('/')
        val inS = PipedInputStream()
        val outS = PipedOutputStream()
        inS.connect(outS)
        s3.putObject(authority, key, inS, ObjectMetadata())
        return outS
    }

}