#!/usr/bin/env python3

""" @package docstring
JABS - Just Another Backup Script - Backup recentness checking tool

Monitors a folder for JABS backups or snapshots and notifies an email address when
the backup is outdated.

Installation:
- Copy jabs-date-checker.cfg in /etc/jabs/jabs-date-checker.cfg and customize it

Usage:
Place a cron entry like this one:

0 5 * * *     root    /usr/local/bin/jabs-date-checker.py /etc/jabs/jabs-date-checker.cfg -q

The script will end silently when has nothing to do.
When the monitored backup is outside of its grace period, an email is sent to alert sysadmin.

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
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import smtplib
import socket
import getpass


NAME = 'jabs-date-checker'
VERSION = '0.1-alpha'
DESCRIPTION = 'Backup recentness checking tool for JABS'
EMAIL_SUBJECT_TPL = 'Backup of {} is TOO OLD!'
EMAIL_MESSAGE_TPL = 'Last successfull run for backup set {} is {}: the backup may be outdated.'
TIMESTAMP_FILENAME = 'backup-timestamp'

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
			if section == 'DEFAULT':
				continue

			s = Settings(section, self.config)
			if s.disabled:
				self._log.info('Ignoring disabled set "{}"'.format(section))
				continue

			self._log.info('Checking date for set "{}"'.format(section))

			# Read date file
			f = open(os.path.join(s.path, TIMESTAMP_FILENAME), "r")
			backupDateStr = f.read().strip()
			backupDate = datetime.strptime(backupDateStr, '%Y-%m-%d %H:%M:%S.%f')
			gpDate = datetime.now() - timedelta(days=int(s.gracePeriod))

			logging.info('Last backup date is {}, grace period is {}'.format(backupDate, gpDate))

			# Check date isn't older than user-defined value
			if backupDate < gpDate:
				# Backup is older
				logging.info('Backup is too old: sending mail')
				self.sendMail(s, backupDate)
			else:
				logging.info('Backup is still valid. Nothing to do')

	def sendMail(self, s, backupDate):
		if s.smtphost:
			logging.info("Sending detailed logs to %s via  %s", s.mailto, s.smtphost)
		else:
			logging.info("Sending detailed logs to %s using local smtp", s.mailto)

		# Create main message
		msg = MIMEMultipart()
		msg['Subject'] = EMAIL_SUBJECT_TPL.format(s.name)
		if s.mailfrom:
			m_from = s.mailfrom
		else:
			m_from = s.username + "@" + s.hostname
		msg['From'] = m_from
		msg['To'] = ', '.join(s.mailto)
		msg.preamble = 'This is a multi-part message in MIME format.'

		# Add base text
		body = EMAIL_MESSAGE_TPL.format(s.name, backupDate)
		txt = MIMEText(body)
		msg.attach(txt)

		# Send the message
		if s.smtpssl and s.smtphost:
			smtp = smtplib.SMTP_SSL(s.smtphost, timeout=300)
		else:
			smtp = smtplib.SMTP(timeout=300)

		if s.smtphost:
			smtp.connect(s.smtphost)
		else:
			smtp.connect()
		if s.smtpuser or s.smtppass:
			smtp.login(s.smtpuser, s.smtppass)
		smtp.sendmail(m_from, s.mailto, msg.as_string())
		smtp.quit()

class Settings:
	''' Represents settings for a backup set '''

	EMAIL_LIST_SEP = ','

	def __init__(self, name, config):
		self.config = config
		self.hostname = socket.getfqdn()
		self.username = getpass.getuser()

		## Backup set name
		self.name = name
		## Disabled
		self.disabled = config.getboolean(name, 'DISABLED')
		## Last backup path (mandatory)
		self.path = config.get(name, 'path')
		## Grace period in which the backup is considered still valid
		self.gracePeriod = self.getStr(name, 'notify_older_days', 1)
		## Email server connection data
		self.smtphost = self.getStr(name, 'SMTPHOST', None)
		self.smtpuser = self.getStr(name, 'SMTPUSER', None)
		self.smtppass = self.getStr(name, 'SMTPPASS', None)
		self.smtpssl = config.getboolean(name, 'SMTPSSL')
		## List of email address to notify about backup status (mandatory)
		mailtoList = config.get(name, 'MAILTO')
		self.mailto = [ x.strip() for x in mailtoList.strip().split(self.EMAIL_LIST_SEP) ]
		## Sender address for the notification email
		self.mailfrom = self.getStr(name, 'MAILFROM', getpass.getuser()+'@'+socket.gethostname())

	def getStr(self, name, key, defaultValue):
		try:
			return self.config.get(name, key)
		except configparser.NoOptionError:
			return defaultValue

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
