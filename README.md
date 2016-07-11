# Spark Strange Refresh Behaviour

Last week I've had some problems when using partitions. Turns out that in some cases the statistics are not properly updated. When explicitly updating the statistics fixes this. The code can be run using: `python -m pytest run.sh`.

## Versions
```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.2
      /_/
```

```
Hive 2.0.1
Subversion git://reznor-mbp-2.local/Users/sergey/git/hivegit -r e3cfeebcefe9a19c5055afdcbb00646908340694
Compiled by sergey on Tue May 3 21:03:11 PDT 2016
From source with checksum 5a49522e4b572555dbbe5dd4773bc7c2
```
