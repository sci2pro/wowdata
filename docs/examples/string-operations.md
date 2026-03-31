# String Operation Examples

These examples show how to use the expanded `string` transform actions for normalization, tokenization, formatting, and byte encoding.

## Normalize Labels

Input:

| raw_name |
|---|
| `  nairobi county ` |
| `mAcHiNe learning` |

Pipeline:

```yaml
wowdata: 0
pipeline:
  start:
    uri: names.csv
    type: csv
  steps:
    - transform:
        op: string
        params:
          column: raw_name
          action: strip
    - transform:
        op: string
        params:
          column: raw_name
          action: title
    - sink:
        uri: cleaned_names.csv
        type: csv
```

Effect:

- `strip` removes surrounding whitespace
- `title` converts the cleaned value to title case

## Remove Prefixes And Suffixes

Input:

| sku |
|---|
| `SKU-001.csv` |
| `SKU-204.csv` |

Pipeline:

```yaml
- transform:
    op: string
    params:
      column: sku
      action: removeprefix
      prefix: "SKU-"
- transform:
    op: string
    params:
      column: sku
      action: removesuffix
      suffix: ".csv"
```

Output values:

- `001`
- `204`

## Replace And Case Normalization

Input:

| category |
|---|
| `HOME_APPLIANCES` |
| `AUDIO_DEVICES` |

Pipeline:

```yaml
- transform:
    op: string
    params:
      column: category
      action: lower
- transform:
    op: string
    params:
      column: category
      action: replace
      old: "_"
      new_value: " "
```

Output values:

- `home appliances`
- `audio devices`

## Split Tokens Into A New Column

Input:

| tags |
|---|
| `climate,rainfall,alert` |
| `ml,vision` |

Pipeline:

```yaml
- transform:
    op: string
    params:
      column: tags
      action: split
      sep: ","
      new: tag_list
```

Output values in `tag_list`:

- `["climate", "rainfall", "alert"]`
- `["ml", "vision"]`

## Partition Structured Codes

Input:

| station_code |
|---|
| `KE-047-NRB` |

Pipeline:

```yaml
- transform:
    op: string
    params:
      column: station_code
      action: partition
      sep: "-"
      new: station_code_parts
```

Output value in `station_code_parts`:

- `("KE", "-", "047-NRB")`

## Format New Display Text

Input:

| template |
|---|
| `Run {name} scored {score}` |

Pipeline:

```yaml
- transform:
    op: string
    params:
      column: template
      action: format
      kwargs:
        name: RedwoodNet-v2
        score: "0.934"
      new: rendered_message
```

Output value in `rendered_message`:

- `Run RedwoodNet-v2 scored 0.934`

## Encode Payloads

Input:

| payload |
|---|
| `hello` |

Pipeline:

```yaml
- transform:
    op: string
    params:
      column: payload
      action: encode
      encoding: utf-8
      new: payload_bytes
```

Output value in `payload_bytes`:

- `b"hello"`

## Padding And Case Variants

Input:

| postal_code | headline |
|---|---|
| `7` | `mIXed Case` |

Pipeline:

```yaml
- transform:
    op: string
    params:
      column: postal_code
      action: zfill
      width: 5
- transform:
    op: string
    params:
      column: headline
      action: swapcase
```

Output values:

- `postal_code`: `00007`
- `headline`: `MixED cASE`
