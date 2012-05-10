# Instructions

## Common instructions

1. Install this project under `/usr/local/src/`
2. Create symlink, depends on python version.

    To find python path, at command line:

        python
        import sys
        sys.path
        exit()

    To create the symlinks:

        ln -s /usr/local/src/karaage-limits/kglimits /usr/local/lib/python2.5/site-packages/kglimits
        ln -s /usr/local/src/karaage-limits/kglimits /usr/local/lib/python2.6/dist-packages/kglimits

3. Test, at command prompt.

        kg-manage shell
        import kglimits.gold
        import kglimits.slurm




## Gold instructions

Skip this section if not using gold.

1. Install gold command line.
2. Ensure gold installed under `/usr/local/gold/bin/` and works as www-data user.
2. Add project to gold that has no access. Call it null_project (or whatever else you want).
3. In `/etc/karaage/global_settings.py` add:

        INSTALLED_APPS += (
            'kglimits.gold',
        )
        GOLD_NULL_PROJECT = null_project

4. Test and fix breakage. Log file /tmp/gold.log will help resolve problems.





## Slurm instructions

Skip this section if not using slurm.

1. Install slurm command line
2. Add to /etc/sudoers:

        www-data ALL=(slurm) NOPASSWD: /usr/local/slurm/latest/bin/sacctmgr

2. Test as www-data

        sudo -uslurm /usr/local/slurm/latest/bin/sacctmgr -ip

3. Install these files in python path.
2. Add project to slurm that has no access. Call it null_project (or whatever else you want).
4. In `/etc/karaage/global_settings.py` add:

        INSTALLED_APPS += (
            'kglimits.slurm',
        )
        SLURM_NULL_PROJECT = null_project

5. Test and fix breakage. Log file /tmp/slurm.log will help resolve problems.
