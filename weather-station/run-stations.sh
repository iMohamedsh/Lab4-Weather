#!/bin/bash

for i in {1..10}
do
    gnome-terminal -- bash -c \
    "java -cp target/classes:$(cat cp.txt) com.weather.WeatherStation $i; exec bash"
done
