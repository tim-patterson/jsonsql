set -e;
./gradlew shadowjar
if [ $GRAAL_NATIVE == 'TRUE' ]; then
   native-image --allow-incomplete-classpath --no-fallback --no-server --report-unsupported-elements-at-runtime -H:EnableURLProtocols=https,http -H:+ReportExceptionStackTraces -R:MaximumHeapSizePercent=20 -jar build/libs/jsonsql.jar
   ./gradlew externalIntegrationTest
fi
./gradlew check