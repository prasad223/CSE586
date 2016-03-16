#!/usr/bin/env python

import os
from environment import *

apk_path = os.path.join(WORKSPACE, PROJECT_NAME, 'app', 'build', 'outputs', 'apk', 'app-debug.apk')
app_package = PREFIX + '.' + PROJECT_EXT

for i in 5554, 5556, 5558, 5560, 5562:
	print "Fresh launch on emulator", i
	emu = "emulator-" + str(i)
	cmd = 'adb -s ' + emu 
	
	#uninstall
	full_cmd =  cmd + ' uninstall ' + app_package 
	os.system(full_cmd)
	
	#install
	full_cmd = cmd + ' install ' + apk_path 
	os.system(full_cmd)
	
	#unlock
	full_cmd = cmd + ' shell input keyevent 82'
	os.system(full_cmd)
	
	#launch
	full_cmd = cmd + ' shell am start -n ' + app_package + '/' + app_package + '.' + MAIN_ACTIVITY
	os.system(full_cmd)

