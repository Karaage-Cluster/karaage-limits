COMMON INSTRUCTIONS:

1. Install this project under /usr/local/src/
2. Create symlink, depends on python version.

To find python path, at command line:

python
import sys
sys.path
exit()

ln -s /usr/local/src/karaage-limits/kglimits /usr/local/lib/python2.5/site-packages/kglimits
ln -s /usr/local/src/karaage-limits/kglimits /usr/local/lib/python2.6/dist-packages/kglimits
3. Test, at command prompt.

python
import kglimits.gold
import kglimits.slurm




GOLD:

1. Install gold command line.
2. Ensure gold installed under /usr/local/gold/bin/ and works as www-data user.
3. In /etc/karaage/global_settings.py add:

INSTALLED_APPS += (
    'kglimits.gold',
)

4. Test and fix breakage. Log file /tmp/gold.log will help resolve problems.





SLURM:

1. Install slurm command line
2. Add to /etc/sudoers:

www-data ALL=(slurm) NOPASSWD: /usr/local/slurm/latest/bin/sacctmgr

2. Test as www-data: sudo -uslurm /usr/local/slurm/latest/bin/sacctmgr -ip
3. Install these files in python path.
4. In /etc/karaage/global_settings.py add:

INSTALLED_APPS += (
    'kglimits.slurm',
)

5. Test and fix breakage. Log file /tmp/slurm.log will help resolve problems.
