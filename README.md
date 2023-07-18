# Python Client Library for Tecton Online Feature Store

A simple Python client for the Feature Server HTTP API that helps customers integrate with Tecton easily.


## Documentation


* [Fetching Online Features](https://docs.tecton.ai/latest/examples/fetch-real-time-features.html)

* [FeatureServer API Reference](https://docs.tecton.ai/rest-swagger/docs.html)

* [Tecton Python Client API Reference](https://tecton-ai.github.io/tecton-http-client-python/html/index.html)

[//]: # (* [Tecton Java Client Example Code]&#40;https://github.com/tecton-ai/TectonClientDemo/tree/main/src/main/java&#41;)


## Troubleshooting


If you have any questions or need help, please [open an Issue](https://github.com/tecton-ai/tecton-http-client-python)
or reach out to us on Slack!

[//]: # ()
[//]: # (## Contributing)

[//]: # ()
[//]: # (The Tecton Java client is open source and we welcome any contributions from our Tecton community.)

[//]: # ()
[//]: # (### Prerequisites)

[//]: # ()
[//]: # (* Java 8 or higher)

[//]: # (* Gradle)

[//]: # (* [Google Java Format]&#40;https://github.com/google/google-java-format&#41; formatter &#40;can also use as a plugin in your IDE&#41;)

[//]: # ()
[//]: # (### Build the Project)

[//]: # ()
[//]: # (The `tecton-http-client-java` project can be built using Gradle as follows:)

[//]: # ()
[//]: # (`./gradlew clean build`)

[//]: # ()
[//]: # (## Basic end to end testing)

[//]: # ()
[//]: # (In the demo client [repository]&#40;https://github.com/tecton-ai/TectonClientDemo&#41; update the `build.gradle` file with the)

[//]: # (jar that you generate from this repo using `./gradlew clean build`.)

[//]: # ()
[//]: # (Change the dependencies target to this and point the files attribute to your java client jar:)

[//]: # ()
[//]: # (```)

[//]: # (dependencies {)

[//]: # (    implementation files&#40;'libs/java-client-0.1.0-SNAPSHOT.jar'&#41;)

[//]: # (    implementation 'com.google.code.gson:gson:2.2.4')

[//]: # (    implementation group: 'org.apache.commons', name: 'commons-lang3', version: '3.12.0')

[//]: # ()
[//]: # (})

[//]: # (```)

[//]: # ()
[//]: # (Update `tecton.properties` with your cluster url and run the Demo file to query the feature services needed.)

[//]: # ()
[//]: # (## Before Opening a PR)

[//]: # ()
[//]: # (* Please run pre-commit on your staged files to ensure that the changes are correctly formatted.)

[//]: # (* Please run `./gradlew clean build` to ensure that your changes pass the build)

[//]: # (* Please add unit tests if possible to test the new code changes)

## License

The project is licensed
under [Apache License 2.0](https://github.com/tecton-ai/tecton-http-client-java/blob/main/LICENSE.md)
