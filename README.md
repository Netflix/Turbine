## Documentation

See the [Wiki](https://github.com/Netflix/Turbine/wiki) for full documentation, examples, operational details and other information.

## Build Status

<a href='https://travis-ci.org/Netflix/Turbine/builds'><img src='https://travis-ci.org/Netflix/Turbine.svg?branch=2.x'></a>

## Bugs and Feedback

For bugs, questions and discussions please use the [Github Issues](https://github.com/Netflix/Turbine/issues).

## Binaries

Binaries and dependency information for Maven, Ivy, Gradle and others can be found at [http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cnetflix%20turbine).

### Library

Dependencies on the library for embedded use are found on Maven Central.

Example for Maven:

```xml
<dependency>
    <groupId>com.netflix.turbine</groupId>
    <artifactId>turbine</artifactId>
    <version>2.minor.patch</version>
</dependency>
```
and for Ivy:

```xml
<dependency org="com.netflix.turbine" name="turbine" rev="2.minor.patch" />
```

### Executable

The standalone executable can also be found on Maven Central or in the Github Releases section.


## Build

* You need Java 8 or later.

To build:

```
$ git clone git@github.com:Netflix/Turbine.git
$ cd Turbine/
$ ./gradlew build
```

 
## LICENSE

Copyright 2014 Netflix, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
