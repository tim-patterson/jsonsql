set -e;
./gradlew shadowjar
if [ $GRAAL_NATIVE == 'TRUE' ]; then
   native-image --allow-incomplete-classpath --no-fallback -H:EnableURLProtocols=https,http -H:+ReportExceptionStackTraces -R:MaximumHeapSizePercent=20 -jar build/libs/jsonsql.jar
fi
./gradlew check