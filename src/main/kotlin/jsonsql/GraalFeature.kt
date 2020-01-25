package jsonsql

import com.oracle.svm.core.annotate.AutomaticFeature
import org.graalvm.nativeimage.hosted.Feature
import org.graalvm.nativeimage.hosted.RuntimeReflection

@AutomaticFeature
class GraalFeature: Feature {
    override fun beforeAnalysis(access: Feature.BeforeAnalysisAccess) {
        // This is needed for jline's TerminalBuilder.getParentProcessCommand
        RuntimeReflection.register(java.lang.ProcessHandle::class.java.getDeclaredField("current"))
        RuntimeReflection.register(java.lang.ProcessHandle::class.java)
        RuntimeReflection.register(java.lang.ProcessHandle::class.java.getDeclaredField("parent"))
    }
}