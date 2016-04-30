# WMIbeat

Welcome to WMIbeat.  WMIbeat is a [beat](https://github.com/elastic/beats) that allows you to run arbitrary WMI queries
and index the results into [elasticsearch](https://github.com/elastic/elasticsearch) so you can monitor Windows machines.

Ensure that this folder is at the following location:
`${GOPATH}/github.com/eskibars`

## Getting Started with WMIbeat
To get running with WMIbeat, run "go build" and then run wmibeat.exe, as in the below `run` section.
If you don't want to build your own, hop over to the "releases" page to download the latest.

### Configuring
To configure the WMI queries to run, you need to change wmibeat.yml.  Working from the default example:

    classes:
    - class: Win32_OperatingSystem
      fields:
      - FreePhysicalMemory
      - FreeSpaceInPagingFiles
      - FreeVirtualMemory
      - NumberOfProcesses
      - NumberOfUsers
    - class: Win32_PerfFormattedData_PerfDisk_LogicalDisk
      fields:
      - Name
      - FreeMegabytes
      - PercentFreeSpace
      - CurrentDiskQueueLength
      - DiskReadsPerSec
      - DiskWritesPerSec
      - DiskBytesPerSec
      - PercentDiskReadTime
      - PercentDiskWriteTime
      - PercentDiskTime
      whereclause: Name != "_Total"
	  objecttitlecolumn: Name
    - class: Win32_PerfFormattedData_PerfOS_Memory
      fields:
      - CommittedBytes
      - AvailableBytes
      - PercentCommittedBytesInUse

We can configure a set of classes, a set of fields per class, and a whereclause.  If there are multiple results, for any WMI class,
WMIbeat will add the results as arrays.  If you need some help with what classes/fields, you can try [WMI Explorer](https://wmie.codeplex.com/).
Note that many of the more interesting classes are "Perf" classes, which has a special checkbox to see in that tool.
	  
### Run

To run WMIbeat with debugging output enabled, run:

```
./wmibeat -c wmibeat.yml -e -d "*"
```

## Build your own Beat
Beats is open source and has a convenient Beat generator, from which this project is based.
For further development, check out the [beat developer guide](https://www.elastic.co/guide/en/beats/libbeat/current/new-beat.html).
