package jsonsql.filesystems

import com.amazonaws.SdkClientException
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import java.io.*
import java.net.URI


object S3FileSystem: FileSystem {

    private val s3 = AmazonS3ClientBuilder.standard()
            .withForceGlobalBucketAccessEnabled(true)
            .withRegion(try { DefaultAwsRegionProviderChain().region } catch (e: SdkClientException) { "ap-southeast-2" })
            .build()

    override fun listDir(path: String): List<String> {
        val s3Uri = URI.create(path)
        val authority = s3Uri.authority
        val prefix = s3Uri.path.trimStart('/')
        val results = mutableListOf<String>()

        var listing = s3.listObjects(authority, prefix)
        results.addAll(listing.objectSummaries.filter { it.size > 0 }.map { "s3://$authority/${it.key}" })

        while(listing.isTruncated) {
            listing = s3.listNextBatchOfObjects(listing)
            results.addAll(listing.objectSummaries.filter { it.size > 0 }.map { "s3://$authority/${it.key}" })
        }
        return results
    }

    override fun read(path: String): InputStream {
        val s3Uri = URI.create(path)
        val authority = s3Uri.authority
        val key = s3Uri.path.trimStart('/')
        return s3.getObject(authority, key).objectContent
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