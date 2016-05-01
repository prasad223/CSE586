#!/bin/bash
PID_FILE='pid.pid'
adb -s emulator-5554 logcat > avd5554 &
PID_54=$!
echo $PID_54 > $PID_FILE
adb -s emulator-5556 logcat > avd5556 &
PID_56=$! 
echo $PID_56 >> $PID_FILE
adb -s emulator-5558 logcat > avd5558 &
PID_58=$!
echo $PID_58 >> $PID_FILE
adb -s emulator-5560 logcat > avd5560 &
PID_60=$!
echo $PID_60 >> $PID_FILE
adb -s emulator-5562 logcat > avd5562 &
PID_62=$!
echo $PID_62 >> $PID_FILE

cat $PID_FILE
