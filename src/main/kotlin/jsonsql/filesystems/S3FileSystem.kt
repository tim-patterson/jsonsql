package jsonsql.filesystems

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import java.io.InputStream


object S3FileSystem: FileSystem {

    private val s3 = AmazonS3ClientBuilder.defaultClient()

    override fun listDir(authority: String, path: String): List<String> {
        val results = mutableListOf<String>()

        var listing = s3.listObjects(authority, path.trimStart('/'))
        results.addAll(listing.objectSummaries.filter { it.size > 0 }.map { "$authority/${it.key}" })

        while(listing.isTruncated) {
            listing = s3.listNextBatchOfObjects(listing)
            results.addAll(listing.objectSummaries.filter { it.size > 0 }.map { "$authority/${it.key}" })
        }
        return results
    }

    override fun open(authority: String, path: String): InputStream {
        return s3.getObject(authority, path.trimStart('/')).objectContent
    }

}