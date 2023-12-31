FROM airbyte/integration-base-java:dev AS build

#RUN yum install -y krb5-workstation krb5-libs krb5-auth-dialog
WORKDIR /airbyte

ENV APPLICATION source-hive

COPY build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1 && rm -rf ${APPLICATION}.tar

FROM airbyte/integration-base-java:dev

WORKDIR /airbyte

ENV APPLICATION source-hive

COPY --from=build /airbyte /airbyte

# Airbyte's build system uses these labels to know what to name and tag the docker images produced by this Dockerfile.
LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/source-hive
