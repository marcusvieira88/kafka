# Introduction

Commands for create the connectors environment:

First select the connector in the confluence site:
https://www.confluent.io/hub/

In the example I used the twitter connector:
https://github.com/jcustenborder/kafka-connect-twitter/blob/master/README.md

Select the releases and download the tar.gz file.
Add the content in the connectors path and fill the config files.

For execute the connector:
 ```
 connect-standalone.sh connect-standalone.properties twitter.properties
 ```
 or
 ```
 run.sh
 ```
