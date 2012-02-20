from django.db.models import signals
from karaage import machines
from karaage import projects
from datetime import datetime
import subprocess
import csv

from django.conf import settings

if not hasattr(settings, 'SLURM_PATH'):
    settings.SLURM_PATH = "/usr/local/slurm/latest/bin/sacctmgr"
if not hasattr(settings, 'SLURM_DEFAULT_PROJECT'):
    settings.SLURM_DEFAULT_PROJECT = "default"

slurm_path = settings.SLURM_PATH
slurm_default_project = settings.SLURM_DEFAULT_PROJECT

import sys
logfile = open('/tmp/slurm.log', 'a')

def log(msg):
    if msg is None:
        print >>logfile, ""
        logfile.flush()
    else:
        print >>logfile, "%s: %s"%(datetime.now(),msg)
        logfile.flush()

# Call remote command with logging
def call(command, ignore_errors=[]):
    c = [ "sudo", "-uslurm", slurm_path, "-ip" ]
    c.extend(command)
    command = c

    log("Call: %s"%(" ".join(command)))
    retcode = subprocess.call(command,stdout=logfile,stderr=logfile)

    if retcode in ignore_errors:
        log("Returned: %d (ignored)"%(retcode))
        return

    if retcode:
        log("Returned: %d (error)"%(retcode))
        log(None)
        raise subprocess.CalledProcessError(retcode, command)

    log("Returned: %d (good)"%(retcode))
    return

# Read CSV delimited input from Slurm
def read_slurm_output(command):
    c = [ "sudo", "-uslurm", slurm_path, "-ip" ]
    c.extend(command)
    command = c

    log("Call: %s"%(" ".join(command)))
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=logfile)

    results = []
    reader = csv.reader(p.stdout,delimiter="|")

    try:
        headers = reader.next()
        print >>logfile, headers
    except StopIteration, e:
        log("headers not found")
        headers = []

    for row in reader:
        print >>logfile, row
        this_row = {}

        i = 0
        for i in range(0,len(headers)):
            key = headers[i]
            value = row[i]
            this_row[key] = value

        results.append(this_row)

    retcode = p.wait()
    if retcode != 0:
        log("Returned: %d (error)"%(retcode))
        log(None)
        raise subprocess.CalledProcessError(retcode, command)

    if len(headers) == 0:
        raise RuntimeError("Command '%s' didn't return any headers."%command)

    log("Returned: %d (good)"%(retcode))
    return results

# Get the user details from Slurm
def get_slurm_user(username):
    cmd = [ "list", "user", "where", "name=%s"%username ]
    results = read_slurm_output(cmd)

    if len(results) == 0:
        return None
    elif len(results) > 1:
        raise RuntimeError("Command returned multiple results for '%s'."%username)

    the_result = results[0]
    if username.lower() != the_result["User"]:
        raise RuntimeError("We expected username '%s' but got username '%s'."%(username,the_result["User"]))

    return the_result

# Get the project details from Slurm
def get_slurm_project(projectname):
    cmd = [ "list", "accounts", "where", "name=%s"%projectname ]
    results = read_slurm_output(cmd)

    if len(results) == 0:
        return None
    elif len(results) > 1:
        raise RuntimeError("Command returned multiple results for '%s'."%projectname)

    the_result = results[0]
    if projectname.lower() != the_result["Account"]:
        raise RuntimeError("We expected projectname '%s' but got projectname '%s'."%(projectname,the_result["Account"]))

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

# Called when account is created/updated
def account_saved(sender, instance, created, **kwargs):
    username = instance.username
    log("account_saved '%s','%s'"%(username,created))

    # retrieve default project, or use default value if none
    default_project_name = slurm_default_project
    if instance.default_project is not None:
        default_project_name = instance.default_project.pid

    # account created
    # account updated

    slurm_user = get_slurm_user(username)
    if instance.date_deleted is None:
        # date_deleted is not set, user should exist
        log("account is active")

        if slurm_user is None:
            # create user if doesn't exist
            call(["add","user","accounts=%s"%default_project_name,"defaultaccount=%s"%default_project_name,"name=%s"%username])
        else:
            # or just set default project
            call(["modify","user","set","defaultaccount=%s"%default_project_name,"where","name=%s"%username])

        # add rest of projects user belongs to
        for project in instance.user.project_set.all():
            call(["add","user","name=%s"%username,"accounts=%s"%project.pid])
    else:
        # date_deleted is not set, user should not exist
        log("account is not active")
        if slurm_user is not None:
            # delete Slurm user if account marked as deleted
            call(["delete","user","name=%s"%username])

    log(None)
    return

# Called when account is deleted
def account_deleted(sender, instance, **kwargs):
    username = instance.username
    log("account_deleted '%s'"%(username))

    # account deleted

    slurm_user = get_slurm_user(username)
    if slurm_user is not None:
        call(["delete","user","name=%s"%username])

    log(None)
    return

# Setup account hooks
signals.post_save.connect(account_saved, sender=machines.models.UserAccount)
signals.post_delete.connect(account_deleted, sender=machines.models.UserAccount)

# Called when project is saved/updated
def project_saved(sender, instance, created, **kwargs):
    pid = instance.pid
    log("project_saved '%s','%s'"%(instance,created))

    # project created
    # project updated

    if instance.is_active:
        # project is not deleted
        log("project is active")
        slurm_project = get_slurm_project(pid)
        if slurm_project is None:
            call(["add","account","name=%s"%pid,"grpcpumins=0"])
    else:
        # project is deleted
        log("project is not active")
        slurm_project = get_slurm_project(pid)
        if slurm_project is not None:
            call(["delete","account","name=%s"%pid])

    log(None)
    return

# Called when project is deleted
def project_deleted(sender, instance, **kwargs):
    pid = instance.pid
    log("project_deleted '%s'"%(instance))

    # project deleted

    slurm_project = get_slurm_project(pid)
    if slurm_project is not None:
        call(["delete","account","name=%s"%pid])

    log(None)
    return

# Called when m2m changed between user and project
def user_project_changed(sender, instance, action, reverse, model, pk_set, **kwargs):
    log("user_project_changed '%s','%s','%s','%s','%s'"%(instance, action, reverse, model, pk_set))

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
                    log("add user '%s' to project '%s'"%(username,projectname))
                    call(["add","user","accounts=%s"%projectname,"name=%s"%username])
        else:
            projectname = instance.pid
            for user in model.objects.filter(pk__in=pk_set):
                username = user.username
                # If Slurm user does not exist, there is nothing for us to do.
                # Slurm account may not be created yet or it may have been deleted.
                slurm_user = get_slurm_user(username)
                if slurm_user is not None:
                    log("add user '%s' to project '%s'"%(username,projectname))
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
                    log("delete user '%s' to project '%s'"%(username,projectname))
                    call(["delete","user","name=%s"%username,"account=%s"%projectname])
        else:
            projectname = instance.pid
            for user in model.objects.filter(pk__in=pk_set):
                username = user.username
                # If Slurm user does not exist, there is nothing for us to do.
                # Slurm account may not be created yet or it may have been deleted.
                slurm_user = get_slurm_user(username)
                if slurm_user is not None:
                    log("delete user '%s' to project '%s'"%(username,projectname))
                    call(["delete","user","name=%s"%username,"account=%s"%projectname])

    elif action == "post_clear":
        if reverse:
            username = instance.username
            projects = get_slurm_projects_in_user(username)
            for projectname in projects:
                log("remove user '%s' all projects - now processing project '%s'"%(username,projectname))
                call(["delete","user","name=%s"%username,"account=%s"%projectname])
        else:
            projectname = instance.pid
            users = get_slurm_users_in_project(projectname)
            for username in users:
                log("remove project '%s' all users - now processing user '%s'"%(username, projectname))
                call(["delete","user","name=%s"%username,"account=%s"%projectname])

    log(None)
    return

# Setup project hooks
signals.post_save.connect(project_saved, sender=projects.models.Project)
signals.post_delete.connect(project_deleted, sender=projects.models.Project)
signals.m2m_changed.connect(user_project_changed, sender=projects.models.Project.users.through)
