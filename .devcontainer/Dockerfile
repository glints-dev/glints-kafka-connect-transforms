# From: https://github.com/mozilla/docker-sbt/blob/main/Dockerfile

# This Dockerfile has two required ARGs to determine which base image
# to use for the JDK and which sbt version to install.

ARG OPENJDK_TAG=8u292
FROM openjdk:${OPENJDK_TAG}

ARG SBT_VERSION=1.5.7

ARG USERNAME=devcontainer

# Install sbt
RUN \
    mkdir /working/ && \
    cd /working/ && \
    curl -L -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb && \
    dpkg -i sbt-$SBT_VERSION.deb && \
    rm sbt-$SBT_VERSION.deb && \
    apt-get update -y && \
    apt-get install -y sbt && \
    rm -r /working/

RUN useradd -ms /bin/bash $USERNAME && \
    apt-get update -y && \
    apt-get install -y sudo && \
    echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME && \
    chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME

RUN cd /home/$USERNAME && \
    sbt sbtVersion
