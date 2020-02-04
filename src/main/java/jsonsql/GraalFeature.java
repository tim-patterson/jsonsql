package jsonsql;

import com.oracle.svm.core.annotate.AutomaticFeature;
import com.oracle.svm.reflect.hosted.ReflectionMetadataFeature;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeClassInitialization;
import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.*;
import org.slf4j.impl.StaticLoggerBinder;

@AutomaticFeature
public class GraalFeature implements Feature {
    @Override
    public void beforeAnalysis(BeforeAnalysisAccess access) {
        System.out.println("Custom feature GraalFeature.beforeAnalysis being run");
        try {
            // Done to stop graal complaining about the KafkaSub
            RuntimeClassInitialization.initializeAtBuildTime(SubstituteLoggerFactory.class);
            RuntimeClassInitialization.initializeAtBuildTime(LoggerFactory.class);
            RuntimeClassInitialization.initializeAtBuildTime(NOPLoggerFactory.class);
            RuntimeClassInitialization.initializeAtBuildTime(NOPLogger.class);
            RuntimeClassInitialization.initializeAtBuildTime(Util.class);
            RuntimeClassInitialization.initializeAtBuildTime(MarkerIgnoringBase.class);
            RuntimeClassInitialization.initializeAtBuildTime(StaticLoggerBinder.class);

            RuntimeClassInitialization.initializeAtBuildTime("org.apache.kafka", "net.jpountz");

            RuntimeReflection.register(org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
            RuntimeReflection.register(ByteArrayDeserializer.class.getConstructor());
            //RuntimeReflection.register(ByteArrayDeserializer.class.getMethod("deserialize", String.class, byte[].class));

            RuntimeReflection.register(RangeAssignor.class);
            RuntimeReflection.register(RangeAssignor.class.getConstructor());
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
    }
}
