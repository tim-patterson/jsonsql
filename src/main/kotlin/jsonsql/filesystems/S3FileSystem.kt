package jsonsql.filesystems

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import java.io.InputStream


object S3FileSystem: FileSystem {

    private val s3 = AmazonS3ClientBuilder.defaultClient()

    override fun listDir(authority: String, path: String): List<String> {
        val listing = s3.listObjects(authority, path.trimStart('/'))
        return listing.objectSummaries.map { "$authority/${it.key}" }
    }

    override fun open(authority: String, path: String): InputStream {
        return s3.getObject(authority, path.trimStart('/')).objectContent
    }

}