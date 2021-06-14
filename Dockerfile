FROM openjdk:8

ENV APP_DIR /opt/odinson

RUN apt-get --assume-yes update && \
    mkdir /opt/app

ENV ODINSON_VERSION 0.4.0

WORKDIR $APP_DIR

COPY ./extra/target/universal/odinson-extra-$ODINSON_VERSION.zip $APP_DIR/app.zip

RUN unzip -q $APP_DIR/app.zip && \
    export APP=$(ls -d */ | grep extra) && \
    mv $APP app

ENTRYPOINT $APP_DIR/app/bin/shell -J-Xms4g -J-Xmx12g
