from django.db.models import signals
from karaage import people
from karaage import machines
from karaage import projects
from datetime import datetime
import subprocess
import csv

from django.conf import settings

import logging

if not hasattr(settings, 'SLURM_PREFIX'):
    settings.SLURM_PREFIX = [ "sudo", "-uslurm" ]
if not hasattr(settings, 'SLURM_PATH'):
    settings.SLURM_PATH = "/usr/local/slurm/latest/bin/sacctmgr"
if not hasattr(settings, 'SLURM_DEFAULT_PROJECT'):
    settings.SLURM_DEFAULT_PROJECT = "default"

slurm_prefix = settings.SLURM_PREFIX
slurm_path = settings.SLURM_PATH
slurm_default_project = settings.SLURM_DEFAULT_PROJECT

logger = logging.getLogger(__name__)


# used for filtering description containing \n and \r
def filter_string(value):
    if value is None:
        value = ""

    # replace whitespace with space
    value = value.replace("\n"," ")
    value = value.replace("\t"," ")

    # CSV seperator
    value = value.replace("|"," ")

    # remove leading/trailing whitespace
    value = value.strip()

    # Used for stripping non-ascii characters
    value = ''.join(c for c in value if ord(c) > 31)

    return value

def truncate(value, arg):
    """
    Truncates a string after a given number of chars  
    Argument: Number of chars to truncate after
    """
    length = int(arg)
    if value is None:
        value = ""
    if (len(value) > length):
        return value[:length] + "..."
    else:
        return value

# Call remote command with logging
def call(command, ignore_errors=[]):
    c = []
    c.extend(slurm_prefix)
    c.extend([ slurm_path, "-ip" ])
    c.extend(command)
    command = c

    logger.debug("Cmd %s"%command)
    null = open('/dev/null', 'w')
    retcode = subprocess.call(command,stdout=null,stderr=null)
    null.close()

    if retcode in ignore_errors:
        logger.debug("<-- Cmd %s returned %d (ignored)"%(command,retcode))
        return

    if retcode:
        logger.error("<-- Cmd %s returned: %d (error)"%(command,retcode))
        raise subprocess.CalledProcessError(retcode, command)

    logger.debug("<-- Returned %d (good)"%(retcode))
    return

# Read CSV delimited input from Slurm
def read_slurm_output(command):
    c = []
    c.extend(slurm_prefix)
    c.extend([ slurm_path, "-ip" ])
    c.extend(command)
    command = c

    logger.debug("Cmd %s"%command)
    null = open('/dev/null', 'w')
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=null)
    null.close()

    results = []
    reader = csv.reader(p.stdout,delimiter="|")

    try:
        headers = reader.next()
        logger.debug("<-- headers %s"%headers)
    except StopIteration, e:
        logger.debug("Cmd %s headers not found"%command)
        headers = []

    for row in reader:
        logger.debug("<-- row %s"%row)
        this_row = {}

        i = 0
        for i in range(0,len(headers)):
            key = headers[i]
            value = row[i]
            this_row[key] = value

        results.append(this_row)

    retcode = p.wait()
    if retcode != 0:
        logger.error("<-- Cmd %s returned %d (error)"%(command,retcode))
        raise subprocess.CalledProcessError(retcode, command)

    if len(headers) == 0:
        logger.error("Cmd %s didn't return any headers."%command)
        raise RuntimeError("Cmd %s didn't return any headers."%command)

    logger.debug("<-- Returned: %d (good)"%(retcode))
    return results

# Get the user details from Slurm
def get_slurm_user(username):
    cmd = [ "list", "user", "where", "name=%s"%username ]
    results = read_slurm_output(cmd)

    if len(results) == 0:
        return None
    elif len(results) > 1:
        logger.error("Command returned multiple results for '%s'."%username)
        raise RuntimeError("Command returned multiple results for '%s'."%username)

    the_result = results[0]
    the_name = the_result["User"]
    if username.lower() != the_name.lower():
        logger.error("We expected username '%s' but got username '%s'."%(username,the_name))
        raise RuntimeError("We expected username '%s' but got username '%s'."%(username,the_name))

    return the_result

# Get the project details from Slurm
def get_slurm_project(projectname):
    cmd = [ "list", "accounts", "where", "name=%s"%projectname ]
    results = read_slurm_output(cmd)

    if len(results) == 0:
        return None
    elif len(results) > 1:
        logger.error("Command returned multiple results for '%s'."%projectname)
        raise RuntimeError("Command returned multiple results for '%s'."%projectname)

    the_result = results[0]
    the_project = the_result["Account"]
    if projectname.lower() != the_project.lower():
        logger.error("We expected projectname '%s' but got projectname '%s'."%(projectname,the_project))
        raise RuntimeError("We expected projectname '%s' but got projectname '%s'."%(projectname,the_project))

    return the_result

def get_slurm_users_in_project(projectname):
    cmd = [ "list", "assoc", "where", "account=%s"%projectname ]
    results = read_slurm_output(cmd)

    user_list = []
    for v in results:
        if v["User"] != "":
            user_list.append(v["User"])
    return user_list

def get_slurm_projects_in_user(username):
    cmd = [ "list", "assoc", "where", "user=%s"%username ]
    results = read_slurm_output(cmd)

    project_list = []
    for v in results:
        project_list.append(v["Account"])
    return project_list

# Called when person is created/updated
def person_saved(sender, instance, created, **kwargs):
    logger.debug("person_saved '%s','%s'"%(instance.username,created))

    # update user meta information
    if instance.is_active:
        for ua in instance.useraccount_set.filter(date_deleted__isnull=True):
            pass

    logger.debug("returning")
    return

signals.post_save.connect(person_saved, sender=people.models.Person)

# Called when account is created/updated
def account_saved(sender, instance, created, **kwargs):
    username = instance.username
    logger.debug("account_saved '%s','%s'"%(username,created))

    # retrieve default project, or use default value if none
    default_project_name = slurm_default_project
    if instance.default_project is not None:
        default_project_name = instance.default_project.pid

    # account created
    # account updated

    slurm_user = get_slurm_user(username)
    if instance.date_deleted is None:
        # date_deleted is not set, user should exist
        logger.debug("account is active")

        if slurm_user is None:
            # create user if doesn't exist
            call(["add","user","accounts=%s"%default_project_name,"defaultaccount=%s"%default_project_name,"name=%s"%username])
        else:
            # or just set default project
            call(["modify","user","set","defaultaccount=%s"%default_project_name,"where","name=%s"%username])

        # update user meta information

        # add rest of projects user belongs to
        for project in instance.user.project_set.all():
            call(["add","user","name=%s"%username,"accounts=%s"%project.pid])
    else:
        # date_deleted is not set, user should not exist
        logger.debug("account is not active")
        if slurm_user is not None:
            # delete Slurm user if account marked as deleted
            call(["delete","user","name=%s"%username])

    logger.debug("returning")
    return

# Called when account is deleted
def account_deleted(sender, instance, **kwargs):
    username = instance.username
    logger.debug("account_deleted '%s'"%(username))

    # account deleted

    slurm_user = get_slurm_user(username)
    if slurm_user is not None:
        call(["delete","user","name=%s"%username])

    logger.debug("returning")
    return

# Setup account hooks
signals.post_save.connect(account_saved, sender=machines.models.UserAccount)
signals.post_delete.connect(account_deleted, sender=machines.models.UserAccount)

# Called when project is saved/updated
def project_saved(sender, instance, created, **kwargs):
    pid = instance.pid
    logger.debug("project_saved '%s','%s'"%(instance,created))

    # project created
    # project updated

    if instance.is_active:
        # project is not deleted
        logger.debug("project is active")
        slurm_project = get_slurm_project(pid)
        if slurm_project is None:
            call(["add","account","name=%s"%pid,"grpcpumins=0"])

        # update project meta information
        name = truncate(instance.name, 40)
        call(["modify","account","set","Description=%s"%filter_string(name),"where","name=%s"%pid])
        call(["modify","account","set","Organization=%s"%filter_string(instance.institute.name),"where","name=%s"%pid])
    else:
        # project is deleted
        logger.debug("project is not active")
        slurm_project = get_slurm_project(pid)
        if slurm_project is not None:
            call(["delete","account","name=%s"%pid])

    logger.debug("returning")
    return

# Called when project is deleted
def project_deleted(sender, instance, **kwargs):
    pid = instance.pid
    logger.debug("project_deleted '%s'"%(instance))

    # project deleted

    slurm_project = get_slurm_project(pid)
    if slurm_project is not None:
        call(["delete","account","name=%s"%pid])

    logger.debug("returning")
    return

# Called when m2m changed between user and project
def user_project_changed(sender, instance, action, reverse, model, pk_set, **kwargs):
    logger.debug("user_project_changed '%s','%s','%s','%s','%s'"%(instance, action, reverse, model, pk_set))

    if action == "post_add":
        if reverse:
            username = instance.username
            # If Slurm user does not exist, there is nothing for us to do.
            # Slurm account may not be created yet or it may have been deleted.
            slurm_user = get_slurm_user(username)
            if slurm_user is not None:
                username = slurm_user["Name"]
                for project in model.objects.filter(pk__in=pk_set):
                    projectname = project.pid
                    logger.debug("add user '%s' to project '%s'"%(username,projectname))
                    call(["add","user","accounts=%s"%projectname,"name=%s"%username])
        else:
            projectname = instance.pid
            for user in model.objects.filter(pk__in=pk_set):
                username = user.username
                # If Slurm user does not exist, there is nothing for us to do.
                # Slurm account may not be created yet or it may have been deleted.
                slurm_user = get_slurm_user(username)
                if slurm_user is not None:
                    logger.debug("add user '%s' to project '%s'"%(username,projectname))
                    call(["add","user","accounts=%s"%projectname,"name=%s"%username])

    elif action == "post_remove":
        if reverse:
            username = instance.username
            # If Slurm user does not exist, there is nothing for us to do.
            # Slurm account may not be created yet or it may have been deleted.
            slurm_user = get_slurm_user(username)
            if slurm_user is not None:
                for project in model.objects.filter(pk__in=pk_set):
                    projectname = project.pid
                    logger.debug("delete user '%s' to project '%s'"%(username,projectname))
                    call(["delete","user","name=%s"%username,"account=%s"%projectname])
        else:
            projectname = instance.pid
            for user in model.objects.filter(pk__in=pk_set):
                username = user.username
                # If Slurm user does not exist, there is nothing for us to do.
                # Slurm account may not be created yet or it may have been deleted.
                slurm_user = get_slurm_user(username)
                if slurm_user is not None:
                    logger.debug("delete user '%s' to project '%s'"%(username,projectname))
                    call(["delete","user","name=%s"%username,"account=%s"%projectname])

    elif action == "post_clear":
        if reverse:
            username = instance.username
            projects = get_slurm_projects_in_user(username)
            for projectname in projects:
                logger.debug("remove user '%s' all projects - now processing project '%s'"%(username,projectname))
                call(["delete","user","name=%s"%username,"account=%s"%projectname])
        else:
            projectname = instance.pid
            users = get_slurm_users_in_project(projectname)
            for username in users:
                logger.debug("remove project '%s' all users - now processing user '%s'"%(username, projectname))
                call(["delete","user","name=%s"%username,"account=%s"%projectname])

    logger.debug("returning")
    return

# Setup project hooks
signals.post_save.connect(project_saved, sender=projects.models.Project)
signals.post_delete.connect(project_deleted, sender=projects.models.Project)
signals.m2m_changed.connect(user_project_changed, sender=projects.models.Project.users.through)
