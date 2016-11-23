# Lonlat filter plugin for Embulk

緯度経度(lon/lat)の表記を変換するプラグイン


## Overview

* **Plugin type**: filter

## Configuration

- **option1**: description (integer, required)
- **option2**: description (string, default: `"myvalue"`)
- **option3**: description (string, default: `null`)

## Example

```yaml
filters:
  - type: lonlat
    option1: example1
    option2: example2
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
