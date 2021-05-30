#!/usr/bin/env python3

""" @package docstring
JABS - Just Another Backup Script - Backup recentness checking tool

Monitors a folder for JABS backups and notifies an email address when
the backup is outdated.

@author Daniele Verducci <daniele.verducci@gmail.com>
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
"""

import os
import sys
import logging
import traceback
import subprocess
import configparser

NAME = 'jabs-date-checker'
VERSION = '0.1-alpha'
DESCRIPTION = 'Backup recentness checking tool for JABS'

class Main:
	''' The recentness checker program '''

	TIMESTAMP_FILE = 'backup-timestamp'

	def __init__(self, configPath):
		''' Reads the config '''
		self._log = logging.getLogger('main')

		if not os.path.exists(configPath) or not os.path.isfile(configPath):
			raise ValueError('configPath must be a file')

		self.config = configparser.ConfigParser()
		self.config.read(configPath)

	def run(self):
		''' Runs the date checker '''
		# Checks sets and snapshots them
		for section in self.config:
			self._log.info(section)
			if section == 'DEFAULT':
				continue

			self._log.info('Checking date for set "{}"'.format(section))
			path = self.config.get(section, 'path')

			# Read date file
			f = open(path, "r")
			print(f.read())

			# Check date isn't older than user-defined value

			# Send email



if __name__ == '__main__':
	import argparse

	parser = argparse.ArgumentParser(
		prog = NAME,
		description = DESCRIPTION,
		formatter_class = argparse.ArgumentDefaultsHelpFormatter
	)
	parser.add_argument('configFile', help="configuration file path")
	parser.add_argument('-q', '--quiet', action='store_true', help="suppress non-essential output")
	args = parser.parse_args()

	if args.quiet:
		level = logging.WARNING
	else:
		level = logging.INFO
	format = r"%(name)s: %(message)s"
	logging.basicConfig(level=level, format=format)

	try:
		main = Main(args.configFile).run()
	except Exception as e:
		logging.critical(traceback.format_exc())
		print('ERROR: {}'.format(e))
		sys.exit(1)

	sys.exit(0)
