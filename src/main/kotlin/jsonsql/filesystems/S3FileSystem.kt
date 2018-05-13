package jsonsql.filesystems

import com.amazonaws.SdkClientException
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import jsonsql.functions.StringInspector
import java.io.*
import java.net.URI


object S3FileSystem: StreamFileSystem() {

    private val s3 = AmazonS3ClientBuilder.standard()
            .withForceGlobalBucketAccessEnabled(true)
            .withRegion(try { DefaultAwsRegionProviderChain().region } catch (e: SdkClientException) { "ap-southeast-2" })
            .build()

    override fun listDir(path: String): List<Map<String, Any?>> {
        val s3Uri = URI.create(path)
        val authority = s3Uri.authority
        val prefix = s3Uri.path.trimStart('/')
        val results = mutableListOf<Map<String, Any?>>()

        var listing = s3.listObjects(authority, prefix)
        results.addAll(listing.objectSummaries.map {
            mapOf(
                "path" to "s3://$authority/${it.key}",
                "bucket" to it.bucketName,
                "key" to it.key,
                "owner" to it.owner.displayName,
                "storage_class" to it.storageClass,
                "last_modified" to StringInspector.inspect(it.lastModified.toInstant()),
                "size" to it.size
            )
        })

        while(listing.isTruncated) {
            listing = s3.listNextBatchOfObjects(listing)
            results.addAll(listing.objectSummaries.map {
                mapOf(
                        "path" to "s3://$authority/${it.key}",
                        "bucket" to it.bucketName,
                        "key" to it.key,
                        "owner" to it.owner.displayName,
                        "storage_class" to it.storageClass,
                        "last_modified" to it.lastModified,
                        "size" to it.size
                )
            })
        }
        return results
    }


    override fun read(path: String): Iterator<InputStream> {
        return listDir(path).filter { it["size"] != 0 }.asSequence().map {
            s3.getObject(it["bucket"] as String, it["key"] as String).objectContent
        }.iterator()

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