set -e;
./gradlew shadowjar
docker run -it -v $(pwd):/app oracle/graalvm-ce sh -c "gu install native-image && cd /app && time native-image --allow-incomplete-classpath --no-fallback -H:EnableURLProtocols=https,http -H:+ReportExceptionStackTraces -jar build/libs/jsonsql.jar && bash"