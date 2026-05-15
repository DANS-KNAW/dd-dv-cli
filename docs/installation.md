Installation
============

Currently, this project is built as an RPM package for RHEL8/Rocky8 and later. The RPM will install the binaries to
`/opt/dans.knaw.nl/dd-dv-cli` and the configuration files to `/etc/opt/dans.knaw.nl/dd-dv-cli`.

Building from source
--------------------

Prerequisites:

* Java 17 or higher
* Maven 3.3.3 or higher
* RPM

Steps:

```bash
git clone https://github.com/DANS-KNAW/dd-dv-cli.git
cd dd-dv-cli 
mvn clean install
```
