#! /usr/bin/env python
# -*- coding: utf-8 -*-

""" @package docstring
JABS - Just Another Backup Script

This is a simple and powerful rsync-based backup script.

Main features:
- Rsync-based: Bandwidth is optimized during transfers
- Automatic "Hanoi" backup set rotation
- Incremental "complete" backups using hard links
- E-Mail notifications on completion

Installation:
- This script is supposed to run as root
- Copy jabs.cfg in /etc/jabs/jabs.cfg and customize it

Usage:
Place a cron entry like this one:

MAILTO="your-email-address"

*/5 * * * *     root    /usr/local/bin/jabs.py -b -q

The script will end silently when has nothing to do.
Where there is a "soft" error or when a backup is completed, you'll receive
an email from the script
Where there is an "hard" error, you'll receive an email from Cron Daemon (so
make sure cron is able to send emails)

@author Gabriele Tozzi <gabriele@tozzi.eu>

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

import os, sys, socket, subprocess, tempfile, getpass
from stat import S_ISDIR, S_ISLNK, ST_MODE
from optparse import OptionParser
import ConfigParser
from string import Template
from time import sleep, mktime
from datetime import datetime, date, timedelta, time
import re
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Default configuration
configfile = "/etc/jabs/jabs.cfg"
version = "jabs v.1.0.2"
cachedir = "/var/cache/jabs"

# Useful regexp
rpat = re.compile('{setname}')
rdir = re.compile('{dirname}')
risremote = re.compile('(.*@.*):(.*)')
rlsparser = re.compile('^([^\s]+)\s+([0-9]+)\s+([^\s]+)\s+([^\s]+)\s+([0-9]+)\s+([0-9]{4}-[0-9]{2}-[0-9]{2}\s[0-9]{2}:[0-9]{2})\s+(.+)$')

# ------------ FUNCTIONS AND CLASSES ----------------------

def ifelse(condition, ifTrue, ifFalse):
    """
        Simulates ternary operator
    """
    if condition:
        return ifTrue
    else:
        return ifFalse

class MyLogger:
    """ Custom logger class

        Assumed debug levels:
        -2: ERROR message
        -1: WARNING message
        0: NORMAL message
        1: SUPERFLUOUS message
        2: DEBUG message
    """
    def __init__(self):
        #List of lists: 0: the string, 1=debug level
        self.logs = []
        #With print only messages with this debug level or lower
        self.debuglvl = 0

    def setdebuglvl(self, lvl):
        self.debuglvl = lvl

    def add(self,*args,**kwargs):
        """
            Adds a line to self.logs and eventually prints it
            Arguments are passed like in the print() builtin
            function. An optional 'lvl' named argument may be
            specified to set debug level (default: 0)
            Also, an optional 'noprint' parameter may be set
            to true to avoid printing that message regardless
            of debug level
        """
        if 'lvl' in kwargs:
            lvl = kwargs['lvl']
        else:
            lvl = 0
        outstr = ''
        for arg in args:
            outstr += str(arg) + " "
        if len(outstr):
            outstr = outstr[:-1]
        if lvl <= self.debuglvl and not ( 'noprint' in kwargs and kwargs['noprint'] ):
            print outstr
        self.logs.append([outstr, lvl])

    def getstr(self,lvl=0):
        """ Returns the buffered log as string """
        retstr = ''
        for l in self.logs:
            if l[1] <= lvl:
                retstr += l[0] + "\n"
        return retstr

def wrapper(func, args):
    """
        Takes a list of arguments and passes them as positional arguments to
        func
    """
    return func(*args)

def getConfigValue(name, config, backupset, default=None):
    """
        Reads a value from configuration,
        try to read it from backupset section, reads it from Global if missing
        returns the value or exits with an error
    """
    try:
        return config.get(backupset, name).strip()
    except(ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        try:
            return config.get("Global", name).strip()
        except:
            if default != None:
                return default
            else:
                print "Error parsing config file: option", name, "not found."
                sys.exit(10)

def parseInterval(string):
    """
        Reads a string interval, i.e. "1d 1h 1m 1s" and returns a timedelta
    """
    d, h, m, s = (0 for x in xrange(4))
    if len(string):
        for i in string.split():
            if i[-1] == 's':
                s = int(i[:-1])
            elif i[-1] == 'm':
                m = int(i[:-1])
            elif i[-1] == 'h':
                h = int(i[:-1])
            elif i[-1] == 'd':
                d = int(i[:-1])
    return timedelta(days=d,hours=h,minutes=m,seconds=s)

def parseTwoTimes(string):
    """
        Reads a string in the format hh:mm:ss-hh:mm:ss, returns a list of time objects
    """
    return map(lambda s: wrapper(time,map(int,s.split(':'))), string.split('-'))

def splitAndTrim(string):
    """
        Split a string by commas and trim each element of the resulting list.
        If input is None, return None
    """
    if string is None:
        return string
    return map(lambda i: i.strip(), string.split(','))

class BackupSet:
    """
        Backup set class
    """
    
    def __init__(self, name, config):
        self.disabled = False
        self.name = name
        self.backuplist = None
        self.ionice = 0
        self.nice = 0
        self.rsync_opts = None
        self.src = None
        self.dst = None
        self.sleep = 0
        self.hanoi = 0
        self.hanoiday = "1970-01-01"
        self.hardlink = False
        self.checkdst = False
        self.sep = "."
        self.pri = 0
        self.datefile = ""
        self.interval = ""
        self.ping = False
        self.runtime = "00:00:00-23:59:59"
        self.mailto = None
        self.mailfrom = getpass.getuser() + '@' + socket.gethostname()
        self.mount = ""
        self.umount = ""
        
        self.backuplist = getConfigValue('BACKUPLIST', config, self.name).split(",")
        self.backuplist = [ x.strip() for x in self.backuplist ]
        self.ionice = int(getConfigValue('IONICE', config, self.name, self.ionice))
        self.nice = int(getConfigValue('NICE', config, self.name, self.nice))
        self.rsync_opts = getConfigValue('RSYNC_OPTS', config, self.name).split(",")
        self.rsync_opts = [ x.strip() for x in self.rsync_opts ]
        self.src = getConfigValue('SRC', config, self.name)
        self.dst = getConfigValue('DST', config, self.name)
        self.sleep = int(getConfigValue('SLEEP', config, self.name, self.sleep))
        self.hanoi = int(getConfigValue('HANOI', config, self.name, self.hanoi))
        self.hanoiday = getConfigValue('HANOIDAY', config, self.name, self.hanoiday)
        self.hanoiday = wrapper(date, map(int, self.hanoiday.split('-',3)))
        self.hardlink = bool(getConfigValue('HARDLINK', config, self.name, self.hardlink))
        self.checkdst = bool(getConfigValue('CHECKDST', config, self.name, self.checkdst))
        self.sep = getConfigValue('SEP', config, self.name, self.sep)
        self.pri = int(getConfigValue('PRI', config, self.name, self.pri))
        self.datefile = getConfigValue('DATEFILE', config, self.name, self.datefile)
        self.interval = parseInterval(getConfigValue('INTERVAL', config, self.name, self.interval))
        self.ping = bool(getConfigValue('PING', config, self.name, self.ping))
        self.runtime = parseTwoTimes(getConfigValue('RUNTIME', config, self.name, self.runtime))
        self.mailto = splitAndTrim(getConfigValue('MAILTO', config, self.name, self.mailto))
        self.mailfrom = getConfigValue('MAILFROM', config, self.name, self.mailfrom)
        self.mount = getConfigValue('MOUNT', config, self.name, self.mount)
        self.umount = getConfigValue('UMOUNT', config, self.name, self.umount)
        self.disabled = bool(getConfigValue('DISABLED', config, self.name, self.disabled))

# ----------------------------------------------------------

# ------------ INIT ---------------------------------------

# Init some useful variables
hostname = socket.gethostname()
username = getpass.getuser()
starttime = datetime.now()

# Parses the command line
usage = "usage: %prog [options] [sets]"
version = version
parser = OptionParser(usage=usage, version=version)
parser.add_option("-c", "--config", dest="configfile",
    help="Config file name (default: " + configfile + ")")
parser.add_option("-a", "--cachedir", dest="cachedir",
    help="Cache directory (default: " + cachedir + ")")
parser.add_option("-d", "--debug", dest="debug", type="int",
    help="Debug level (0 to 1, default: 0)")
parser.add_option("-q", "--quiet", dest="quiet", action="store_true",
    help="suppress all non-error output (overrides -d)")
parser.add_option("-f", "--force", dest="force", action="store_true",
    help="ignore time constraints: will always run sets at any time")
parser.add_option("-b", "--batch", dest="batch", action="store_true",
    help="batch mode: exit silently if script is already running")
parser.add_option("-s", "--safe", dest="safe", action="store_true",
    help="safe mode: just print what will do, don't change anything")
parser.set_defaults(configfile=configfile,cachedir=cachedir,debug=0)

(options, args) = parser.parse_args()

# Set debug level to -1 if --quiet was specified
if options.quiet:
    options.debug = -1

# Validate the command line
#if not options.setname:
#    parser.print_help()

# Reads the config file
config = ConfigParser.ConfigParser()
try:
    config.readfp(open(options.configfile))
except IOError:
    print "ERROR: Couldn't open config file", options.configfile
    parser.print_help()
    sys.exit(1)

# Reads settings from the config file
sets = config.sections()
if sets.count("Global") < 1:
    print "ERROR: Global section on config file not found"
    sys.exit(1)
sets.remove("Global")

# If specified at command line, remove unwanted sets
if args:
    lower_args = map(lambda i: i.lower(), args)
    sets[:] = [s for s in sets if s.lower() in lower_args]

if options.debug > 0:
    print "Will run these backup sets:", sets

#Init backup sets
newsets = []
for s in sets:
    newsets.append(BackupSet(s, config))
sets = newsets
del newsets

#Sort backup sets by priority
sets = sorted(sets, key=lambda s: s.pri)

#Read the PIDFILE
pidfile = getConfigValue('PIDFILE', config, None)

# Check if another insnance of the script is already running
if os.path.isfile(pidfile):
    PIDFILE = open(pidfile, "r")
    try:
        os.kill(int(PIDFILE.read()), 0)
    except:
        # The process is no longer running, ok
        PIDFILE.close()
    else:
        # The other process is till running
        if options.batch:
            sys.exit(0)
        else:
            print "Error: this script is already running!"
            sys.exit(12)

# Save my PID on pidfile
try:
    PIDFILE = open(pidfile, "w")
except:
    print "Error: couldn't open PID file", pidfile
    sys.exit(15)
PIDFILE.write(str(os.getpid()))
PIDFILE.flush()

# Remove disabled sets
newsets = []
for s in sets:
    if not s.disabled:
        newsets.append(s)
sets = newsets
del newsets

# Check for sets to run based on current time
if not options.force:
    newsets = []
    for s in sets:
        if s.runtime[0] > starttime.time() or s.runtime[1] < starttime.time():
            if options.debug > 0:
                print "Skipping set", s.name, "because out of runtime (", s.runtime[0].isoformat(), "-", s.runtime[1].isoformat(), ")"
        else:
            newsets.append(s)
    sets = newsets
    del newsets

# Check for sets to run based on interval
if not options.force:
    newsets = []
    for s in sets:
        if s.interval > timedelta(seconds=0):
            # Check if its time to run this set
            if options.debug > 0:
                print "Will run", s.name, "every", s.interval
            cachefile = options.cachedir + "/" + s.name
            if not os.path.exists(options.cachedir):
                print "WARNING: Cache directory missing, creating it"
                os.mkdir(os.path.dirname(cachefile))
            if not os.path.exists(cachefile):
                lastdone = datetime.fromtimestamp(0)
                print "WARNING: Last backup timestamp for", s.name, "is missing. Assuming 01-01-1970"
            else:
                CACHEFILE = open(cachefile,'r')
                try:
                    lastdone = datetime.fromtimestamp(int(CACHEFILE.readline()))
                except ValueError:
                    print "WARNING: Last backup timestamp for", s.name, "corrupted. Assuming 01-01-1970"
                    lastdone = datetime.fromtimestamp(0)
                CACHEFILE.close()
            if options.debug > 0:
                print "Last", s.name, "run:", lastdone

            if lastdone + s.interval > starttime:
                if options.debug > 0:
                    print "Skipping set", s.name, "because interval not reached (", str(lastdone+s.interval-starttime), "still remains )"
            else:
                newsets.append(s)

    sets = newsets
    del newsets

# Ping hosts if required
newsets = []
for s in sets:
    rem = risremote.match(s.src)
    if s.ping and rem:
        host = rem.group(1).split('@')[1]
        if options.debug > 0:
            print "Pinging host", host
        FNULL = open('/dev/null', 'w')
        hup = subprocess.call(['ping', '-c 3','-n','-w 60', host], stdout=FNULL, stderr=FNULL)
        FNULL.close()
        if hup == 0:
            if options.debug > -1:
                print host, "is UP."
            newsets.append(s)
        elif options.debug > 0:
            print "Skipping backup of", host, "because it's down."
    else:
        newsets.append(s)

sets = newsets
del newsets

# Check if some set is still remaining after checks
if not len(sets):
    sys.exit(0)

# Print the backup header
backupheader_tpl = Template("""
-------------------------------------------------
$version

Backup of $hostname
Backup date: $starttime
Backup sets:
$backuplist
-------------------------------------------------

""")

nicelist = ""
for s in sets:
    nicelist = nicelist + "  " + s.name + "\n"
if len(nicelist) > 0:
    nicelist = nicelist[:-1]

backupheader = backupheader_tpl.substitute(
    version = version,
    hostname = hostname,
    starttime = starttime.ctime(),
    backuplist = nicelist,
)
if options.debug > -1:
    print backupheader

# ---------------- DO THE BACKUP ---------------------------

for s in sets:

    sstarttime = datetime.now()

    # Write some log data in a string, to be eventually mailed later
    sl = MyLogger()
    sl.setdebuglvl(options.debug)
    sl.add(backupheader_tpl.substitute(
        version = version,
        hostname = hostname,
        starttime = starttime.ctime(),
        backuplist = s.name,
    ), noprint=True)

    sl.add("")

    if s.mount:
        if os.path.ismount(s.mount):
            sl.add("WARNING: Skipping mount of", s.mount, "because it's already mounted", lvl=-1)
        else:
            # Mount specified location
            cmd = ["mount", s.mount ]
            sl.add("Mounting", s.mount)
            p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            ret = p.poll()
            if ret != 0:
                sl.add("WARNING: Mount of", s.mount, "failed with return code", ret, lvl=-1)

    if s.checkdst:
        # Checks whether the given backup destination exists
        try:
            i = os.path.exists(s.dst)
            if not i:
                sl.add("WARNING: Skipping", s.name, "set, destination", s.dst, "not found.", lvl=-1)
                continue
        except:
            sl.add("WARNING: Skipping", s.name, "set, read error on", s.dst, ".", lvl=-1)
            continue

    # Put a file cointaining backup date on dest dir
    tmpdir = tempfile.mkdtemp()
    tmpfile = None
    if len(s.datefile):
        if options.safe:
            sl.add("Skipping creation of datefile", s.datefile)
        else:
            tmpfile = tmpdir + "/" + s.datefile
            sl.add("Generating datefile", tmpfile)
            TMPFILE = open(tmpfile,"w")
            TMPFILE.write(str(datetime.now())+"\n")
            TMPFILE.close()
            s.backuplist.append(tmpfile)

    # Calculate curret hanoi day and suffix to use
    hanoisuf = ""
    if s.hanoi > 0:
        today = (starttime.date() - s.hanoiday).days + 1
        i = s.hanoi
        while i >= 0:
            if today % 2 ** i == 0:
                hanoisuf = chr(i+65)
                break
            i -= 1
        sl.add("First hanoi day:", s.hanoiday, lvl=1)
        sl.add("Hanoi sets to use:", s.hanoi)
        sl.add("Today is hanoi day", today, "- using suffix:", hanoisuf)

    plink = []
    if s.hardlink:
        # Seek for most recent backup set to hard link
        rem = risremote.match(s.dst)
        if rem:
            #Backing up to a remote path
            (path, base) = os.path.split(rem.group(2))
            sl.add("Backing up to remote path:", rem.group(1), rem.group(2), lvl=1)
            cmd = ["ssh", "-o", "BatchMode=true", rem.group(1), "ls -l --color=never --time-style=long-iso -t -1 \"" + path + "\"" ]
            sl.add("Issuing remote command:", cmd)
            p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            sl.add("Subprocess return code:", p.poll())
            if len(stderr):
                sl.add("WARNING: stderr was not empty:", stderr, lvl=-1)
            files = stdout.split('\n')
            psets = []
            for f in files:
                m = rlsparser.match(f)
                # If file matched regexp and is a directory
                if m and m.group(1)[0] == "d":
                    btime = datetime.strptime(m.group(6),"%Y-%m-%d %H:%M")
                    psets.append([m.group(7),btime])
        else:
            (path, base) = os.path.split(s.dst)
            dirs = os.listdir(path)
            psets = []
            for d in dirs:
                if ( d == base or d[:len(base)] + s.sep == base + s.sep ) and S_ISDIR(os.lstat(path+"/"+d)[ST_MODE]):
                    btime = datetime.fromtimestamp(os.stat(path+"/"+d).st_mtime)
                    psets.append([d,btime])
            psets = sorted(psets, key=lambda pset: pset[1], reverse=True) #Sort by age
        
        for p in psets:
            sl.add("Found previous backup:", p[0], "(", p[1], ")", lvl=1)
            if p[0] != base + s.sep + hanoisuf:
                plink.append(path + "/" + p[0])
        
        if len(plink):
            sl.add("Will hard link against", plink)
        else:
            sl.add("Will NOT use hard linking (no suitable set found)")
    
    else:
        sl.add("Will NOT use hark linking (disabled)")
    
    tarlogs = []
    setsuccess = True
    for d in s.backuplist:
        sl.add("Backing up", d, "on", s.name, "...")
        tarlogfile = None
        if s.mailto:
            tarlogfile = tmpdir + '/' + re.sub(r'(\/|\.)', '_', s.name + '-' + d) + '.log'
        if not options.safe:
            tarlogs.append(tarlogfile)
        
        #Build command line
        
        cmd, cmdi, cmdn, cmdr = ([] for x in xrange(4))
        cmdi.extend(["ionice", "-c", str(s.ionice)])
        cmdn.extend(["nice", "-n", str(s.nice)])
        cmdr.append("rsync")
        cmdr.extend(map(lambda x: rpat.sub(s.name.lower(),x), s.rsync_opts))
        for pl in plink:
            cmdr.append("--link-dest=" + pl )
        if tmpfile and d == tmpfile:
            cmdr.append(tmpfile)
        else:
            cmdr.append(rdir.sub(d, s.src))
        cmdr.append(rdir.sub(d, s.dst+ifelse(len(hanoisuf)>0,s.sep+hanoisuf,"")))
        
        if s.ionice != 0:
            cmd.extend(cmdi)
        if s.nice != 0:
            cmd.extend(cmdn)
        cmd.extend(cmdr)
        
        if options.safe:
            nlvl = 0
        else:
            nlvl = 1
        sl.add("Commandline:", cmd, lvl=nlvl)
        sl.add("Will write tar STDOUT to", tarlogfile, lvl=1)
        
        if not options.safe:
            sys.stdout.flush()
            TARLOGFILE = open(tarlogfile, 'wb')
            try:
                p = subprocess.Popen(cmd,stdout=TARLOGFILE,stderr=subprocess.PIPE)
            except OSError as e:
                print "ERROR: Unable to locate file", e.filename
                print "Path: ", os.environ['PATH']
                sys.exit(1)
            stdout, stderr = p.communicate()
            ret = p.poll()
            TARLOGFILE.close()
            if ret != 0 or len(stderr) > 0:
                setsuccess = False
            sl.add("Done. Exit status:", ret)
            if len(stderr):
                sl.add("ERROR: stderr was not empty:", -1)
                sl.add(stderr, -1)
    
        if s.sleep > 0:
            if options.safe:
                sl.add("Should sleep", s.sleep, "secs now, skipping.")
            else:
                sl.add("Sleeping", s.sleep, "secs.")
                sleep(s.sleep)

    # Save last backup execution time
    if s.interval > timedelta(seconds=0):
        if options.safe:
            sl.add("Skipping write of last backup timestamp")
        else:
            sl.add("Writing last backup timestamp", lvl=1)
            
            # Create cachedir if missing
            if not os.path.exists(options.cachedir):
                os.makedirs(options.cachedir, 0700)
            
            cachefile = options.cachedir + os.sep + s.name
            CACHEFILE = open(cachefile,'w')
            CACHEFILE.write(str(int(mktime(starttime.timetuple())))+"\n")
            CACHEFILE.close()
    
    # Create backup symlink, is using hanoi and not remote
    if len(hanoisuf)>0 and not rem:
        if os.path.exists(s.dst) and S_ISLNK(os.lstat(s.dst)[ST_MODE]):
            if options.safe:
                sl.add("Skipping deletion of old symlink", s.dst)
            else:
                sl.add("Deleting old symlink", s.dst)
                os.unlink(s.dst)
        if not os.path.exists(s.dst):
            if options.safe:
                sl.add("Skipping creation of symlink", s.dst, "to", s.dst+s.sep+hanoisuf)
            else:
                sl.add("Creating symlink", s.dst, "to", s.dst+s.sep+hanoisuf)
                os.symlink(s.dst+s.sep+hanoisuf, s.dst)
        elif not options.safe:
            sl.add("WARNING: Can't create symlink", s.dst, "a file with such name exists", lvl=-1)

    stooktime = datetime.now() - sstarttime
    sl.add("Set", s.name, "completed. Took:", stooktime)
    
    # Umount
    if s.umount:
        if not os.path.ismount(s.umount):
            sl.add("WARNING: Skipping umount of", s.umount, "because it's not mounted", lvl=-1)
        else:
            # Umount specified location
            cmd = ["umount", s.umount ]
            sl.add("Umounting", s.umount)
            p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            stdout, stderr = p.communicate()
            ret = p.poll()
            if ret != 0:
                sl.add("WARNING: Umount of", s.umount, "failed with return code", ret, lvl=-1)

    # Send email
    if s.mailto:
        if options.safe:
            sl.add("Skipping sending detailed logs to", s.mailto)
        else:
            sl.add("Sending detailed logs to", s.mailto)

            # Creo il messaggio principale
            msg = MIMEMultipart()
            if setsuccess:
                i = "OK"
            else:
                i = "FAILED"
            msg['Subject'] = "Backup of " + s.name + " " + i
            if s.mailfrom:
                m_from = s.mailfrom
            else:
                m_from = username + "@" + hostname
            msg['From'] = m_from
            msg['To'] = ', '.join(s.mailto)
            msg.preamble = 'This is a milti-part message in MIME format.'
            
            # Aggiungo il testo base
            txt = sl.getstr() + "\n\nDetailed logs are attached.\n"
            txt = MIMEText(txt)
            msg.attach(txt)

            # Aggiungo gli allegati
            for tl in tarlogs:
                if tl:
                    TL = open(tl, 'rb')
                    att = MIMEText(TL.read(),'plain','utf-8')
                    TL.close()
                    att.add_header(
                        'Content-Disposition',
                        'attachment',
                        filename=os.path.basename(tl)
                    )
                    msg.attach(att)
            
            # Invio il messaggio
            smtp = smtplib.SMTP()
            smtp.connect()
            smtp.sendmail(m_from, s.mailto, msg.as_string())
            smtp.quit()

    # Cancello eventuali log temporanei
    for tl in tarlogs:
        if tl:
            sl.add("Deleting log file", tl, lvl=1)
            os.unlink(tl)
    tarlogs = []

    # Delete tmpfile, if created
    if tmpfile and len(tmpfile):
        sl.add("Deleting temporary files")
        os.unlink(tmpfile)
    if tmpdir:
        os.rmdir(tmpdir)

took = datetime.now() - starttime
if options.debug > -1:
    print "Backup completed. Took", took

sys.exit(0)

