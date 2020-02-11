package jsonsql;

import com.amazonaws.partitions.model.*;
import com.amazonaws.services.s3.internal.AWSS3V4Signer;
import com.fasterxml.jackson.databind.ext.Java7HandlersImpl;
import com.oracle.svm.core.annotate.AutomaticFeature;
import com.oracle.svm.core.jdk.proxy.DynamicProxyRegistry;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.apache.commons.logging.impl.NoOpLog;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.pool.ConnPoolControl;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.graalvm.nativeimage.ImageSingletons;
import org.graalvm.nativeimage.hosted.Feature;
import org.graalvm.nativeimage.hosted.RuntimeClassInitialization;
import org.graalvm.nativeimage.hosted.RuntimeReflection;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.*;
import org.slf4j.impl.StaticLoggerBinder;

import java.util.HashSet;

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

            RuntimeReflection.register(RangeAssignor.class);
            RuntimeReflection.register(RangeAssignor.class.getConstructor());


            // For aws libs
            System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
            RuntimeClassInitialization.initializeAtBuildTime("org.apache.commons.logging");
            RuntimeReflection.register(NoOpLog.class);
            RuntimeReflection.register(NoOpLog.class.getConstructor(), NoOpLog.class.getConstructor(String.class));

            RuntimeReflection.register(LogFactoryImpl.class);
            // Not 100% sure why it needs this, I think some reflective methods must recursively call superclass reflective calls or something
            RuntimeReflection.register(LogFactory.class);
            RuntimeReflection.registerForReflectiveInstantiation(LogFactoryImpl.class);
            // This is a weird one!. Needed as it uses the string class to do a reflective constructor lookup
            // Graal should have figured this out but maybe due to it being captured in an array or something it didn't
            RuntimeReflection.register(String.class);

            // This seems to be only loaded sometimes to deal with errors or something?
            RuntimeReflection.register(Class.forName("com.sun.xml.internal.stream.XMLInputFactoryImpl"));

            // These are pojo's instantiated by jackson from json data
            runtimeReflectionRegisterAll(Partitions.class);
            runtimeReflectionRegisterAll(Partition.class);
            runtimeReflectionRegisterAll(Region.class);
            runtimeReflectionRegisterAll(Service.class);
            runtimeReflectionRegisterAll(Endpoint.class);
            runtimeReflectionRegisterAll(CredentialScope.class);
            RuntimeReflection.register(HashSet.class.getConstructors());
            RuntimeReflection.register(Java7HandlersImpl.class.getConstructor());

            // Attempt to init all the aws classes at build time that do all the local json loading stuff
            RuntimeClassInitialization.initializeAtBuildTime("com.amazonaws");
            RuntimeClassInitialization.initializeAtBuildTime("com.fasterxml.jackson");
            RuntimeClassInitialization.initializeAtBuildTime("org.joda.time");
            RuntimeClassInitialization.initializeAtBuildTime("org.apache.http");
            RuntimeClassInitialization.initializeAtBuildTime("org.apache.commons.codec");
            DynamicProxyRegistry dynamicProxySupport = ImageSingletons.lookup(DynamicProxyRegistry.class);
            Class<?> wrapped = Class.forName("com.amazonaws.http.conn.Wrapped");
            dynamicProxySupport.addProxyClass(HttpClientConnectionManager.class, ConnPoolControl.class, wrapped);

            RuntimeReflection.register(AWSS3V4Signer.class.getConstructors());

        } catch (NoSuchMethodException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Used to register pojo's used for json deserialization
     */
    private static <T> void runtimeReflectionRegisterAll(Class<T> klass) {
        RuntimeReflection.register(klass.getConstructors());
        RuntimeReflection.register(klass.getFields());
        RuntimeReflection.register(klass.getMethods());
    }
}
