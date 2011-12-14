from django.db.models import signals
from karaage import machines
from karaage import projects
from datetime import datetime
import subprocess
import csv

gold = "/usr/local/gold/bin"

import sys
logfile = open('/tmp/gold.log', 'a')

def log(msg):
    if msg is None:
        print >>logfile, ""
        logfile.flush()
    else:
        print >>logfile, "%s: %s"%(datetime.now(),msg)
        logfile.flush()

# Call remote command with logging
def call(command, ignore_errors=[]):
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

# Read CSV delimited input from Gold
def read_gold_output(command):
    log("Call: %s"%(" ".join(command)))
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=logfile)

    results = {}
    reader = csv.reader(p.stdout,delimiter="|")

    try:
        headers = reader.next()
        print >>logfile, headers
    except StopIteration, e:
        log("headers not found")
        headers = []

    for row in reader:
        print >>logfile, row
        name = row[0].lower()

        if name in results:
            log(None)
            raise RuntimeError("We found name '%s' returned from '%s' multiple times."%(name,command))

        results[name] = {}

        i = 0
        for i in range(0,len(headers)):
            key = headers[i]
            value = row[i]
            results[name][key] = value

    retcode = p.wait()
    if retcode != 0:
        log("Returned: %d (error)"%(retcode))
        log(None)
        raise subprocess.CalledProcessError(retcode, command)

    log("Returned: %d (good)"%(retcode))
    return results

# Get the user details from Gold
def get_gold_user(username):
    cmd = [ "%s/glsuser"%(gold), "-u", username, "--raw" ]
    results = read_gold_output(cmd)

    username = username.lower()
    if username in results:
        log("Found '%s'"%(results))
        return results[username]
    else:
        log("Not found")
        return None

# Get the project details from Gold
def get_gold_user_balance(username):
    cmd = [ "%s/gbalance"%(gold), "-u", username, "--raw" ]
    results = read_gold_output(cmd)
    return results

# Get the project details from Gold
def get_gold_project(projectname):
    cmd = [ "%s/glsproject"%(gold), "-p", projectname, "--raw" ]
    results = read_gold_output(cmd)

    projectname = projectname.lower()
    if projectname in results:
        log("Found '%s'"%(results))
        return results[projectname]
    else:
        log("Not found")
        return None

def get_gold_users_in_project(projectname):
    gold_project = get_gold_project(projectname)
    if gold_project is None:
        log("error '%s'"%(projectname))
        log(None)
        raise RuntimeError("Project '%s' does not exist in gold"%(projectname))

    if gold_project["Users"] == "":
        return []
    else:
        return gold_project["Users"].lower().split(",")

def get_gold_projects_in_user(username):
    gold_balance = get_gold_user_balance(username)
    if gold_balance is None:
        log("error '%s'"%(username))
        log(None)
        raise RuntimeError("User '%s' does not exist in gold"%(username))

    projects = []
    for k, v in gold_balance.iteritems():
        projects.append(v["Name"])
    return projects

# Called when account is created/updated
def account_saved(sender, instance, created, **kwargs):
    default_project_name = instance.default_project.pid
    username = instance.username
    log("account_saved '%s','%s'"%(username,created))

    # account created
    # account updated

    gold_user = get_gold_user(username)
    if instance.date_deleted is None:
        # date_deleted is not set, user should exist
        log("account is active")
        if gold_user is None:
            call(["%s/gmkuser"%(gold),"-A","-p",default_project_name,"-u",username])
        else:
            call(["%s/gchuser"%(gold),"-p",default_project_name,"-u",username])

        for project in instance.user.project_set.all():
            call(["%s/gchproject"%(gold),"--addUsers",username,"-p",project.pid],ignore_errors=[74])
    else:
        # date_deleted is not set, user should not exist
        log("account is not active")
        if gold_user is not None:
            # delete gold user if account marked as deleted
            call(["%s/grmuser"%(gold),"-u",username],ignore_errors=[8])

    log(None)
    return

# Called when account is deleted
def account_deleted(sender, instance, **kwargs):
    username = instance.username
    log("account_deleted '%s'"%(username))

    # account deleted

    gold_user = get_gold_user(username)
    if gold_user is not None:
        call(["%s/grmuser"%(gold),"-u",username],ignore_errors=[8])

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
        gold_project = get_gold_project(pid)
        if gold_project is None:
            call(["%s/gmkproject"%(gold),"-p",pid,"-u","MEMBERS"])
    else:
        # project is deleted
        log("project is not active")
        gold_project = get_gold_project(pid)
        if gold_project is not None:
            call(["%s/grmproject"%(gold),"-p",pid])

    log(None)
    return

# Called when project is deleted
def project_deleted(sender, instance, **kwargs):
    pid = instance.pid
    log("project_deleted '%s'"%(instance))

    # project deleted

    gold_project = get_gold_project(pid)
    if gold_project is not None:
        call(["%s/grmproject"%(gold),"-p",pid])

    log(None)
    return

# Called when m2m changed between user and project
def user_project_changed(sender, instance, action, reverse, model, pk_set, **kwargs):
    log("user_project_changed '%s','%s','%s','%s','%s'"%(instance, action, reverse, model, pk_set))

    if action == "post_add":
        if reverse:
            username = instance.username
            # If gold user does not exist, there is nothing for us to do.
            # Gold account may not be created yet or it may have been deleted.
            gold_user = get_gold_user(username)
            if gold_user is not None:
                username = gold_user["Name"]
                for project in model.objects.filter(pk__in=pk_set):
                    projectname = project.pid
                    log("add user '%s' to project '%s'"%(username,projectname))
                    call(["%s/gchproject"%(gold),"--addUsers",username,"-p",projectname],ignore_errors=[74])
        else:
            projectname = instance.pid
            for user in model.objects.filter(pk__in=pk_set):
                username = user.username
                # If gold user does not exist, there is nothing for us to do.
                # Gold account may not be created yet or it may have been deleted.
                gold_user = get_gold_user(username)
                if gold_user is not None:
                    log("add user '%s' to project '%s'"%(username,projectname))
                    call(["%s/gchproject"%(gold),"--addUsers",username,"-p",projectname],ignore_errors=[74])

    elif action == "post_remove":
        if reverse:
            username = instance.username
            # If gold user does not exist, there is nothing for us to do.
            # Gold account may not be created yet or it may have been deleted.
            gold_user = get_gold_user(username)
            if gold_user is not None:
                for project in model.objects.filter(pk__in=pk_set):
                    projectname = project.pid
                    log("delete user '%s' to project '%s'"%(username,projectname))
                    call(["%s/gchproject"%(gold),"--delUsers",username,"-p",projectname])
        else:
            projectname = instance.pid
            for user in model.objects.filter(pk__in=pk_set):
                username = user.username
                # If gold user does not exist, there is nothing for us to do.
                # Gold account may not be created yet or it may have been deleted.
                gold_user = get_gold_user(username)
                if gold_user is not None:
                    log("delete user '%s' to project '%s'"%(username,projectname))
                    call(["%s/gchproject"%(gold),"--delUsers",username,"-p",projectname])

    elif action == "post_clear":
        if reverse:
            username = instance.username
            # FIXME! This will list projects with global membership, which
            # can't be deleted.
            # FIXME! What happens to default project?
            projects = get_gold_projects_in_user(username)
            for projectname in projects:
                log("remove user '%s' all projects - now processing project '%s'"%(username,projectname))
                call(["%s/gchproject"%(gold),"--delUsers",username,"-p",projectname])
        else:
            # FIXME! get_gold_users_in_project doesn't return all users in project
            projectname = instance.pid
            users = get_gold_users_in_project(projectname)
            for username in users:
                log("remove project '%s' all users - now processing user '%s'"%(username, projectname))
                call(["%s/gchproject"%(gold),"--delUsers",username,"-p",projectname])

    log(None)
    return

# Setup project hooks
signals.post_save.connect(project_saved, sender=projects.models.Project)
signals.post_delete.connect(project_deleted, sender=projects.models.Project)
signals.m2m_changed.connect(user_project_changed, sender=projects.models.Project.users.through)
