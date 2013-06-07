from django.db.models import signals
from karaage import people
from karaage import machines
from karaage import projects
from datetime import datetime
import subprocess
import csv

from django.conf import settings

import logging

if not hasattr(settings, 'GOLD_PREFIX'):
    settings.GOLD_PREFIX = []
if not hasattr(settings, 'GOLD_PATH'):
    settings.GOLD_PATH = "/usr/local/gold/bin"
if not hasattr(settings, 'GOLD_NULL_PROJECT'):
    settings.GOLD_NULL_PROJECT = "default"

gold_prefix = settings.GOLD_PREFIX
gold_path = settings.GOLD_PATH
gold_null_project = settings.GOLD_NULL_PROJECT

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

    # hack because gold doesn't quote sql correctly
    value = value.replace("'","\\'")

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
    c.extend(gold_prefix)
    c.append("%s/%s"%(gold_path,command[0]))
    c.extend(command[1:])
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

# Read CSV delimited input from Gold
def read_gold_output(command):
    c = []
    c.extend(gold_prefix)
    c.append("%s/%s"%(gold_path,command[0]))
    c.extend(command[1:])
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
        logger.debug("Cmd %s didn't return any headers."%command)

    logger.debug("<-- Returned: %d (good)"%(retcode))
    return results

# Get the user details from Gold
def get_gold_user(username):
    cmd = [ "glsuser", "-u", username, "--raw" ]
    results = read_gold_output(cmd)

    if len(results) == 0:
        return None
    elif len(results) > 1:
        logger.error("Command returned multiple results for '%s'."%username)
        raise RuntimeError("Command returned multiple results for '%s'."%username)

    the_result = results[0]
    the_name = the_result["Name"]
    if username.lower() != the_name.lower():
        logger.error("We expected username '%s' but got username '%s'."%(username,the_name))
        raise RuntimeError("We expected username '%s' but got username '%s'."%(username,the_name))

    return the_result

# Get the user balance details from Gold
def get_gold_user_balance(username):
    cmd = [ "gbalance", "-u", username, "--raw" ]
    results = read_gold_output(cmd)

    if len(results) == 0:
        return None

    return results

# Get the project details from Gold
def get_gold_project(projectname):
    cmd = [ "glsproject", "-p", projectname, "--raw" ]
    results = read_gold_output(cmd)

    if len(results) == 0:
        return None
    elif len(results) > 1:
        logger.error("Command returned multiple results for '%s'."%projectname)
        raise RuntimeError("Command returned multiple results for '%s'."%projectname)

    the_result = results[0]
    the_project = the_result["Name"]
    if projectname.lower() != the_project.lower():
        logger.error("We expected projectname '%s' but got projectname '%s'."%(projectname,the_project))
        raise RuntimeError("We expected projectname '%s' but got projectname '%s'."%(projectname,the_project))

    return the_result

def get_gold_users_in_project(projectname):
    gold_project = get_gold_project(projectname)
    if gold_project is None:
        logger.error("Project '%s' does not exist in Gold"%(projectname))
        raise RuntimeError("Project '%s' does not exist in Gold"%(projectname))

    if gold_project["Users"] == "":
        return []
    else:
        return gold_project["Users"].lower().split(",")

def get_gold_projects_in_user(username):
    gold_balance = get_gold_user_balance(username)
    if gold_balance is None:
        logger.error("User '%s' does not exist in Gold"%(username))
        raise RuntimeError("User '%s' does not exist in Gold"%(username))

    projects = []
    for v in gold_balance:
        projects.append(v["Name"])
    return projects

# Called when institute is created/updated
def institute_saved(sender, instance, created, **kwargs):
    name = instance.name
    logger.debug("institute_saved '%s','%s'"%(name,created))

    # institute created
    # institute updated

    if instance.is_active:
        # date_deleted is not set, user should exist
        logger.debug("institute is active")

        call(["goldsh","Organization","Create","Name=%s"%name],ignore_errors=[185])
    else:
        # date_deleted is not set, user should not exist
        logger.debug("institute is not active")
        # delete Gold organisation if institute marked as deleted
        call(["goldsh","Organization","Delete","Name==%s"%name])

    logger.debug("returning")
    return

# Called when institute is deleted
def institute_deleted(sender, instance, **kwargs):
    name = instance.name
    logger.debug("institute_deleted '%s'"%(name))

    # institute deleted
    call(["goldsh","Organization","Delete","Name==%s"%name])

    logger.debug("returning")
    return

# Setup institute hooks
signals.post_save.connect(institute_saved, sender=people.models.Institute)
signals.post_delete.connect(institute_deleted, sender=people.models.Institute)


# Called when person is created/updated
def person_saved(sender, instance, created, **kwargs):
    logger.debug("person_saved '%s','%s'"%(instance.username,created))

    # update user meta information
    if instance.is_active:
        for ua in instance.useraccount_set.filter(date_deleted__isnull=True):
            call(["gchuser","-n",filter_string(instance.get_full_name()),"-u",ua.username])
            call(["gchuser","-E",filter_string(instance.email),"-u",ua.username])

    logger.debug("returning")
    return

signals.post_save.connect(person_saved, sender=people.models.Person)

# Called when account is created/updated
def account_saved(sender, instance, created, **kwargs):
    username = instance.username
    logger.debug("account_saved '%s','%s'"%(username,created))

    # retrieve default project, or use null project if none
    default_project_name = gold_null_project
    if instance.default_project is not None:
        default_project_name = instance.default_project.pid

    # account created
    # account updated

    gold_user = get_gold_user(username)
    if instance.date_deleted is None:
        # date_deleted is not set, user should exist
        logger.debug("account is active")

        if gold_user is None:
            # create user if doesn't exist
            call(["gmkuser","-A","-p",default_project_name,"-u",username])
        else:
            # or just set default project
            call(["gchuser","-p",default_project_name,"-u",username])

        # update user meta information
        call(["gchuser","-n",filter_string(instance.user.get_full_name()),"-u",username])
        call(["gchuser","-E",filter_string(instance.user.email),"-u",username])

        # add rest of projects user belongs to
        for project in instance.user.project_set.all():
            call(["gchproject","--add-user",username,"-p",project.pid],ignore_errors=[74])
    else:
        # date_deleted is not set, user should not exist
        logger.debug("account is not active")
        if gold_user is not None:
            # delete Gold user if account marked as deleted
            call(["grmuser","-u",username],ignore_errors=[8])

    logger.debug("returning")
    return

# Called when account is deleted
def account_deleted(sender, instance, **kwargs):
    username = instance.username
    logger.debug("account_deleted '%s'"%(username))

    # account deleted

    gold_user = get_gold_user(username)
    if gold_user is not None:
        call(["grmuser","-u",username],ignore_errors=[8])

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
        gold_project = get_gold_project(pid)
        if gold_project is None:
            call(["gmkproject","-p",pid,"-u","MEMBERS"])

        # update project meta information
        name = truncate(instance.name, 40)
        call(["gchproject","-d",filter_string(name),"-p",pid])
        call(["gchproject","-X","Organization=%s"%filter_string(instance.institute.name),"-p",pid])
    else:
        # project is deleted
        logger.debug("project is not active")
        gold_project = get_gold_project(pid)
        if gold_project is not None:
            call(["grmproject","-p",pid])

    logger.debug("returning")
    return

# Called when project is deleted
def project_deleted(sender, instance, **kwargs):
    pid = instance.pid
    logger.debug("project_deleted '%s'"%(instance))

    # project deleted

    gold_project = get_gold_project(pid)
    if gold_project is not None:
        call(["grmproject","-p",pid])

    logger.debug("returning")
    return

# Called when m2m changed between user and project
def user_project_changed(sender, instance, action, reverse, model, pk_set, **kwargs):
    logger.debug("user_project_changed '%s','%s','%s','%s','%s'"%(instance, action, reverse, model, pk_set))

    if action == "post_add":
        if reverse:
            username = instance.username
            # If Gold user does not exist, there is nothing for us to do.
            # Gold account may not be created yet or it may have been deleted.
            gold_user = get_gold_user(username)
            if gold_user is not None:
                username = gold_user["Name"]
                for project in model.objects.filter(pk__in=pk_set):
                    projectname = project.pid
                    logger.debug("add user '%s' to project '%s'"%(username,projectname))
                    call(["gchproject","--add-user",username,"-p",projectname],ignore_errors=[74])
        else:
            projectname = instance.pid
            for user in model.objects.filter(pk__in=pk_set):
                username = user.username
                # If Gold user does not exist, there is nothing for us to do.
                # Gold account may not be created yet or it may have been deleted.
                gold_user = get_gold_user(username)
                if gold_user is not None:
                    logger.debug("add user '%s' to project '%s'"%(username,projectname))
                    call(["gchproject","--add-user",username,"-p",projectname],ignore_errors=[74])

    elif action == "post_remove":
        if reverse:
            username = instance.username
            # If Gold user does not exist, there is nothing for us to do.
            # Gold account may not be created yet or it may have been deleted.
            gold_user = get_gold_user(username)
            if gold_user is not None:
                for project in model.objects.filter(pk__in=pk_set):
                    projectname = project.pid
                    logger.debug("delete user '%s' to project '%s'"%(username,projectname))
                    call(["gchproject","--del-users",username,"-p",projectname])
        else:
            projectname = instance.pid
            for user in model.objects.filter(pk__in=pk_set):
                username = user.username
                # If Gold user does not exist, there is nothing for us to do.
                # Gold account may not be created yet or it may have been deleted.
                gold_user = get_gold_user(username)
                if gold_user is not None:
                    logger.debug("delete user '%s' to project '%s'"%(username,projectname))
                    call(["gchproject","--del-users",username,"-p",projectname])

    elif action == "post_clear":
        if reverse:
            username = instance.username
            # FIXME! This will list projects with global membership, which
            # can't be deleted.
            # FIXME! What happens to default project?
            projects = get_gold_projects_in_user(username)
            for projectname in projects:
                logger.debug("remove user '%s' all projects - now processing project '%s'"%(username,projectname))
                call(["gchproject","--del-users",username,"-p",projectname])
        else:
            # FIXME! get_gold_users_in_project doesn't return all users in project
            projectname = instance.pid
            users = get_gold_users_in_project(projectname)
            for username in users:
                logger.debug("remove project '%s' all users - now processing user '%s'"%(username, projectname))
                call(["gchproject","--del-users",username,"-p",projectname])

    logger.debug("returning")
    return

# Setup project hooks
signals.post_save.connect(project_saved, sender=projects.models.Project)
signals.post_delete.connect(project_deleted, sender=projects.models.Project)
signals.m2m_changed.connect(user_project_changed, sender=projects.models.Project.users.through)
