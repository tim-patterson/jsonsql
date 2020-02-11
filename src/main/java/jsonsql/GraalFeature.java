package jsonsql;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.profile.internal.BasicProfileConfigLoader;
import com.amazonaws.auth.profile.internal.securitytoken.RoleInfo;
import com.amazonaws.internal.config.*;
import com.amazonaws.partitions.model.*;
import com.amazonaws.services.s3.internal.AWSS3V4Signer;
import com.amazonaws.services.securitytoken.internal.STSProfileCredentialsService;
import com.amazonaws.services.securitytoken.model.*;
import com.fasterxml.jackson.databind.ext.Java7HandlersImpl;
import com.oracle.svm.core.annotate.AutomaticFeature;
import com.oracle.svm.core.jdk.LocalizationSupport;
import com.oracle.svm.core.jdk.ResourceBundleSubstitutions;
import com.oracle.svm.core.jdk.proxy.DynamicProxyRegistry;
import jdk.xml.internal.SecuritySupport;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.LogFactoryImpl;
import org.apache.commons.logging.impl.NoOpLog;
import org.apache.http.conn.ConnectionRequest;
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

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.ResourceBundle;

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
            RuntimeReflection.register(Class.forName("com.sun.xml.internal.stream.XMLInputFactoryImpl").getConstructors());
            RuntimeReflection.register(Class.forName("com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl"));
            RuntimeReflection.register(Class.forName("com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl").getConstructors());
            RuntimeReflection.register(com.sun.org.apache.xml.internal.utils.FastStringBuffer.class.getConstructors());

            RuntimeReflection.register(AWSSecurityTokenServiceException.class.getConstructors());
            RuntimeReflection.register(ExpiredTokenException.class.getConstructors());
            RuntimeReflection.register(IDPCommunicationErrorException.class.getConstructors());
            RuntimeReflection.register(IDPRejectedClaimException.class.getConstructors());
            RuntimeReflection.register(InvalidAuthorizationMessageException.class.getConstructors());
            RuntimeReflection.register(InvalidIdentityTokenException.class.getConstructors());
            RuntimeReflection.register(MalformedPolicyDocumentException.class.getConstructors());
            RuntimeReflection.register(PackedPolicyTooLargeException.class.getConstructors());
            RuntimeReflection.register(RegionDisabledException.class.getConstructors());

            // STS support
            RuntimeReflection.register(STSProfileCredentialsService.class);
            RuntimeReflection.register(STSProfileCredentialsService.class.getConstructors());
            RuntimeReflection.register(STSProfileCredentialsService.class.getMethods());
            RuntimeReflection.register(AWS4Signer.class.getConstructors());

            // These are pojo's instantiated by jackson from json data
            runtimeReflectionRegisterAll(Partitions.class);
            runtimeReflectionRegisterAll(Partition.class);
            runtimeReflectionRegisterAll(Region.class);
            runtimeReflectionRegisterAll(Service.class);
            runtimeReflectionRegisterAll(Endpoint.class);
            runtimeReflectionRegisterAll(CredentialScope.class);
            runtimeReflectionRegisterAll(InternalConfigJsonHelper.class);
            runtimeReflectionRegisterAll(SignerConfigJsonHelper.class);
            runtimeReflectionRegisterAll(HttpClientConfigJsonHelper.class);
            runtimeReflectionRegisterAll(JsonIndex.class);
            runtimeReflectionRegisterAll(HostRegexToRegionMappingJsonHelper.class);
            runtimeReflectionRegisterAll(RoleInfo.class);
            RuntimeReflection.register(HashSet.class.getConstructors());
            RuntimeReflection.register(Java7HandlersImpl.class.getConstructor());

            RuntimeClassInitialization.initializeAtBuildTime("com.fasterxml.jackson");
            RuntimeClassInitialization.initializeAtBuildTime("org.joda.time");
            RuntimeClassInitialization.initializeAtBuildTime("org.apache.http");
            RuntimeClassInitialization.initializeAtBuildTime("org.apache.commons.codec");


            DynamicProxyRegistry dynamicProxySupport = ImageSingletons.lookup(DynamicProxyRegistry.class);
            Class<?> wrapped = Class.forName("com.amazonaws.http.conn.Wrapped");
            RuntimeClassInitialization.initializeAtBuildTime(wrapped);

            dynamicProxySupport.addProxyClass(HttpClientConnectionManager.class, ConnPoolControl.class, wrapped);
            dynamicProxySupport.addProxyClass(ConnectionRequest.class, wrapped);

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
